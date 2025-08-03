use std::mem;
use std::time::Duration;

use futures::StreamExt;
use sqlx::Row;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::types::Decimal;
use tokio::sync::mpsc;

use crate::types::{Address, OpInfo};

use super::{Batch, Data, Update};

pub(super) struct Subscribe {
    rx: mpsc::UnboundedReceiver<sqlx::Result<PgRow>>,
    spec: Box<dyn Spec>,
    stash: Vec<Update>,
}

impl Subscribe {
    pub fn start(mut conn: PgConnection, spec: impl Spec) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let query = spec.query();

        tokio::spawn(async move {
            let query = format!("SUBSCRIBE ({query}) WITH (PROGRESS)");
            let mut stream = sqlx::query(&query).fetch(&mut conn);
            while let Some(row) = stream.next().await {
                if tx.send(row).is_err() {
                    break;
                }
            }
        });

        Self {
            rx,
            spec: Box::new(spec),
            stash: Vec::new(),
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn recv(&mut self) -> anyhow::Result<Batch> {
        loop {
            // `mpsc::UnboundedReceiver::recv` is cancel safe.
            let row = self.rx.recv().await.unwrap()?;

            if row.get("mz_progressed") {
                let time = get_mz_timestamp(&row)?;
                return Ok(Batch {
                    time,
                    updates: mem::take(&mut self.stash),
                });
            }

            let update = self.spec.parse_update(&row)?;
            self.stash.push(update);
        }
    }
}

fn get_mz_timestamp(row: &PgRow) -> anyhow::Result<Duration> {
    let ts: Decimal = row.get("mz_timestamp");
    let ms: u64 = ts.try_into()?;
    Ok(Duration::from_millis(ms))
}

pub(super) trait Spec: Send + 'static {
    fn query(&self) -> String;
    fn parse(&self, row: &PgRow) -> anyhow::Result<Data>;

    fn parse_update(&self, row: &PgRow) -> anyhow::Result<Update> {
        let data = self.parse(row)?;
        let time = get_mz_timestamp(row)?;
        let diff = row.get("mz_diff");

        Ok(Update { data, time, diff })
    }
}

pub(super) struct Operator;

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

pub(super) struct Elapsed;

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
}

pub(super) struct Size;

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
