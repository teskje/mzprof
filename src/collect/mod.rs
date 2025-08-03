pub mod subscribe;

use std::any::TypeId;
use std::collections::BTreeMap;
use std::time::Duration;

use async_stream::try_stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use tokio_stream::{StreamMap, StreamNotifyClose};

use crate::collect::subscribe::Subscribe;
use crate::types::{OpId, OpInfo, WorkerId};

pub struct Collector {
    connect_options: PgConnectOptions,
    stream: StreamMap<TypeId, StreamNotifyClose<Subscribe>>,
    progress: BTreeMap<TypeId, Duration>,
    stash: BTreeMap<Duration, Vec<Update>>,
}

impl Collector {
    pub fn new(sql_url: &str, cluster: &str, replica: &str) -> anyhow::Result<Self> {
        let connect_options = sql_url
            .parse::<PgConnectOptions>()?
            .application_name("mzprof")
            .options([("cluster", cluster), ("cluster_replica", replica)]);

        Ok(Self {
            connect_options,
            stream: StreamMap::new(),
            progress: BTreeMap::new(),
            stash: BTreeMap::new(),
        })
    }

    async fn connect(&self) -> anyhow::Result<PgConnection> {
        let conn = PgConnection::connect_with(&self.connect_options).await?;
        Ok(conn)
    }

    pub async fn subscribe(
        &mut self,
        spec: impl subscribe::Spec,
        mode: subscribe::Mode,
    ) -> anyhow::Result<()> {
        let id = spec.type_id();
        let conn = self.connect().await?;
        let sub = Subscribe::start(conn, spec, mode);
        let stream = StreamNotifyClose::new(sub);

        self.stream.insert(id, stream);
        self.progress.insert(id, Duration::ZERO);
        Ok(())
    }

    fn frontier(&self) -> Option<Duration> {
        self.progress.values().copied().min()
    }

    fn absorb_batch(&mut self, id: TypeId, batch: Batch) -> Vec<Batch> {
        self.progress.insert(id, batch.time);

        for update in batch.updates {
            self.stash.entry(update.time).or_default().push(update);
        }

        let frontier = self.frontier().unwrap();
        let mut batches = Vec::new();
        while let Some((time, _)) = self.stash.first_key_value()
            && *time < frontier
        {
            let (time, updates) = self.stash.pop_first().unwrap();
            batches.push(Batch { time, updates });
        }

        batches
    }

    pub fn into_stream(mut self) -> BoxStream<'static, anyhow::Result<Batch>> {
        try_stream! {
            while let Some((id, result)) = self.stream.next().await {
                let Some(result) = result else {
                    self.progress.remove(&id);
                    continue;
                };

                let batch = result?;
                let ready = self.absorb_batch(id, batch);
                for batch in ready {
                    yield batch;
                }
            }
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
pub enum Data {
    Operator(OpId, OpInfo),
    Elapsed(OpId, WorkerId),
    Size(OpId, WorkerId),
}

#[derive(Clone, Debug)]
pub struct Update {
    pub data: Data,
    pub time: Duration,
    pub diff: i64,
}

#[derive(Clone, Debug)]
pub struct Batch {
    pub time: Duration,
    pub updates: Vec<Update>,
}
