use std::time::Duration;

use futures::TryStreamExt;
use sqlx::Row;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::types::Decimal;
use tokio::sync::mpsc;

use crate::types::{Address, Batch, OpInfo, Update, WorkerId};

use super::Event;

pub(super) trait Subscribe: Send + 'static {
    type Data: Send;

    const EVENT_FN: fn(Batch<Self::Data>) -> Event;

    fn query(&self) -> String;
    fn parse(row: &PgRow) -> anyhow::Result<Self::Data>;

    fn parse_update(row: &PgRow) -> anyhow::Result<Update<Self::Data>> {
        let data = Self::parse(row)?;
        let time = parse_timestamp(row)?;
        let diff = row.get("mz_diff");

        Ok(Update { data, time, diff })
    }
}

pub(super) async fn run<S: Subscribe>(
    subs: S,
    mut conn: PgConnection,
    tx: mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let query = format!("SUBSCRIBE ({}) WITH (PROGRESS)", subs.query());
    let mut stream = sqlx::query(&query).fetch(&mut conn);

    let mut updates = Vec::new();
    while let Some(row) = stream.try_next().await? {
        if row.get("mz_progressed") {
            let time = parse_timestamp(&row)?;
            let batch = Batch {
                time,
                updates: std::mem::take(&mut updates),
            };
            let event = S::EVENT_FN(batch);
            if tx.send(event).is_err() {
                break;
            }
        } else {
            let update = S::parse_update(&row)?;
            updates.push(update);
        }
    }

    Ok(())
}

pub(super) struct Elapsed;

impl Subscribe for Elapsed {
    type Data = (OpInfo, WorkerId);

    const EVENT_FN: fn(Batch<Self::Data>) -> Event = Event::Elapsed;

    fn query(&self) -> String {
        "
        SELECT id::int8, worker_id::int8, name, address::text
        FROM mz_introspection.mz_dataflow_operators
        JOIN mz_introspection.mz_dataflow_addresses USING (id)
        JOIN mz_introspection.mz_scheduling_elapsed_raw USING (id)
        "
        .into()
    }

    fn parse(row: &PgRow) -> anyhow::Result<Self::Data> {
        let id: i64 = row.get("id");
        let worker_id: i64 = row.get("worker_id");
        let name = row.get("name");
        let address = parse_address(row)?;

        let id = id.try_into()?;
        let op = OpInfo { id, name, address };
        let worker_id = worker_id.try_into()?;

        Ok((op, worker_id))
    }
}

pub(super) struct Size;

impl Subscribe for Size {
    type Data = (OpInfo, Option<WorkerId>);

    const EVENT_FN: fn(Batch<Self::Data>) -> Event = Event::Size;

    fn query(&self) -> String {
        "
        WITH
            arrangement_sizes AS (
                SELECT id::int8, worker_id::int8, name, address::text
                FROM mz_introspection.mz_dataflow_operators
                JOIN mz_introspection.mz_dataflow_addresses USING (id)
                LEFT JOIN mz_introspection.mz_arrangement_heap_size_raw ON (id = operator_id)
            ),
            batcher_sizes AS (
                SELECT id::int8, worker_id::int8, name, address::text
                FROM mz_introspection.mz_dataflow_operators
                JOIN mz_introspection.mz_dataflow_addresses USING (id)
                LEFT JOIN mz_introspection.mz_arrangement_batcher_size_raw ON (id = operator_id)
            )
        SELECT * FROM arrangement_sizes
        UNION ALL
        SELECT * FROM batcher_sizes
        "
        .into()
    }

    fn parse(row: &PgRow) -> anyhow::Result<Self::Data> {
        let id: i64 = row.get("id");
        let worker_id: Option<i64> = row.get("worker_id");
        let name = row.get("name");
        let address = parse_address(row)?;

        let id = id.try_into()?;
        let op = OpInfo { id, name, address };
        let worker_id = worker_id.map(TryInto::try_into).transpose()?;

        Ok((op, worker_id))
    }
}

fn parse_timestamp(row: &PgRow) -> anyhow::Result<Duration> {
    let ts: Decimal = row.get("mz_timestamp");
    let ms: u64 = ts.try_into()?;
    Ok(Duration::from_millis(ms))
}

fn parse_address(row: &PgRow) -> anyhow::Result<Address> {
    let address: String = row.get("address");
    let indexes = address
        .trim_start_matches('{')
        .trim_end_matches('}')
        .split(',')
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()?
        .into_boxed_slice();

    Ok(Address(indexes))
}
