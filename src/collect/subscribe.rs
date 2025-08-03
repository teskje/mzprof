use std::any::Any;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use async_stream::try_stream;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use sqlx::Row;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::types::Decimal;

use crate::types::{Address, OpInfo};

use super::{Batch, Data, Update};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Snapshot,
    Continual { duration: Option<Duration> },
}

pub(super) struct Subscribe {
    stream: Option<BoxStream<'static, sqlx::Result<PgRow>>>,
    spec: Box<dyn Spec>,
    mode: Mode,
    stash: Vec<Update>,
    up_to: Option<Duration>,
}

impl Subscribe {
    pub fn start(mut conn: PgConnection, spec: impl Spec, mode: Mode) -> Self {
        let query = spec.subscribe_query(mode);
        let stream = try_stream! {
            let mut fetch = sqlx::query(&query).fetch(&mut conn);
            while let Some(row) = fetch.try_next().await? {
                yield row;
            }
        }
        .boxed();

        Self {
            stream: Some(stream),
            spec: Box::new(spec),
            mode,
            stash: Vec::new(),
            up_to: None,
        }
    }

    fn absorb_row(&mut self, row: &PgRow) -> anyhow::Result<Option<Batch>> {
        let progress: bool = row.get("mz_progressed");
        let time = get_mz_timestamp(row)?;

        // The first row communicates the as-of time.
        let Some(up_to) = self.up_to else {
            assert!(progress);
            let up_to = match self.mode {
                Mode::Snapshot => time,
                Mode::Continual { duration: Some(d) } => time + d,
                Mode::Continual { duration: None } => Duration::MAX,
            };
            self.up_to = Some(up_to);
            return Ok(None);
        };

        if progress {
            if time > up_to {
                self.stream = None;
            }

            Ok(Some(Batch {
                time,
                updates: mem::take(&mut self.stash),
            }))
        } else {
            if time <= up_to {
                let update = self.spec.parse_update(row)?;
                self.stash.push(update);
            }

            Ok(None)
        }
    }
}

impl Stream for Subscribe {
    type Item = anyhow::Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(stream) = self.stream.as_mut() else {
                return Poll::Ready(None);
            };

            let result = ready!(stream.poll_next_unpin(cx)).unwrap();
            let result = match result {
                Ok(row) => self.absorb_row(&row),
                Err(error) => Err(error.into()),
            };

            match result {
                Ok(Some(batch)) => break Poll::Ready(Some(Ok(batch))),
                Ok(None) => {}
                Err(error) => break Poll::Ready(Some(Err(error))),
            }
        }
    }
}

fn get_mz_timestamp(row: &PgRow) -> anyhow::Result<Duration> {
    let ts: Decimal = row.get("mz_timestamp");
    let ms: u64 = ts.try_into()?;
    Ok(Duration::from_millis(ms))
}

pub trait Spec: Any + Send + 'static {
    fn query(&self) -> String;
    fn parse(&self, row: &PgRow) -> anyhow::Result<Data>;

    fn parse_update(&self, row: &PgRow) -> anyhow::Result<Update> {
        let data = self.parse(row)?;
        let time = get_mz_timestamp(row)?;
        let diff = row.get("mz_diff");

        Ok(Update { data, time, diff })
    }

    fn subscribe_query(&self, _mode: Mode) -> String {
        format!("SUBSCRIBE ({}) WITH (PROGRESS)", self.query())
    }
}

pub struct Operator;

impl Spec for Operator {
    fn query(&self) -> String {
        "
        SELECT id::int8, name, address::text
        FROM mz_introspection.mz_dataflow_operators
        JOIN mz_introspection.mz_dataflow_addresses USING (id)
        "
        .into()
    }

    fn parse(&self, row: &PgRow) -> anyhow::Result<Data> {
        let id = row.get::<i64, _>("id").try_into()?;
        let name = row.get("name");
        let address: &str = row.get("address");

        let indexes = address
            .trim_start_matches('{')
            .trim_end_matches('}')
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();

        let info = OpInfo {
            name,
            address: Address(indexes),
        };
        Ok(Data::Operator(id, info))
    }
}

pub struct Elapsed;

impl Spec for Elapsed {
    fn query(&self) -> String {
        "
        SELECT id::int8, worker_id::int8
        FROM mz_introspection.mz_scheduling_elapsed_raw
        "
        .into()
    }

    fn parse(&self, row: &PgRow) -> anyhow::Result<Data> {
        let id = row.get::<i64, _>("id").try_into()?;
        let worker_id = row.get::<i64, _>("worker_id").try_into()?;
        Ok(Data::Elapsed(id, worker_id))
    }

    fn subscribe_query(&self, mode: Mode) -> String {
        let snapshot = match mode {
            Mode::Snapshot => "true",
            Mode::Continual { .. } => "false",
        };

        format!(
            "SUBSCRIBE ({}) WITH (PROGRESS, SNAPSHOT = {snapshot})",
            self.query(),
        )
    }
}

pub struct Size;

impl Spec for Size {
    fn query(&self) -> String {
        "
        SELECT operator_id::int8, worker_id::int8
        FROM mz_introspection.mz_arrangement_heap_size_raw
        UNION ALL
        SELECT operator_id::int8, worker_id::int8
        FROM mz_introspection.mz_arrangement_batcher_size_raw
        "
        .into()
    }

    fn parse(&self, row: &PgRow) -> anyhow::Result<Data> {
        let id = row.get::<i64, _>("operator_id").try_into()?;
        let worker_id = row.get::<i64, _>("worker_id").try_into()?;
        Ok(Data::Size(id, worker_id))
    }
}
