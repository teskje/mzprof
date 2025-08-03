mod subscribe;
mod wrapper;

use std::collections::BTreeMap;
use std::time::Duration;

use async_stream::try_stream;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, future};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};

use crate::collect::subscribe::Subscribe;
use crate::types::{OpId, OpInfo, WorkerId};

pub struct Collector {
    connect_options: PgConnectOptions,
    subscribes: Vec<Subscribe>,
    progress: Vec<Duration>,
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
            subscribes: Vec::new(),
            progress: Vec::new(),
            stash: BTreeMap::new(),
        })
    }

    async fn connect(&self) -> anyhow::Result<PgConnection> {
        let conn = PgConnection::connect_with(&self.connect_options).await?;
        Ok(conn)
    }

    pub async fn subscribe_operator(&mut self) -> anyhow::Result<()> {
        self.subscribe(subscribe::Operator).await
    }

    pub async fn subscribe_elapsed(&mut self) -> anyhow::Result<()> {
        self.subscribe(subscribe::Elapsed).await
    }

    pub async fn subscribe_size(&mut self) -> anyhow::Result<()> {
        self.subscribe(subscribe::Size).await
    }

    async fn subscribe(&mut self, spec: impl subscribe::Spec) -> anyhow::Result<()> {
        let conn = self.connect().await?;
        let sub = Subscribe::start(conn, spec);
        self.subscribes.push(sub);
        self.progress.push(Duration::ZERO);
        Ok(())
    }

    fn into_stream(mut self) -> BoxStream<'static, anyhow::Result<Batch>> {
        try_stream! {
            loop {
                let futs = self.subscribes.iter_mut().map(|s| s.recv().boxed());
                let (result, idx, _) = future::select_all(futs).await;
                let batch = result?;

                for update in batch.updates {
                    self.stash.entry(update.time).or_default().push(update);
                }
                self.progress[idx] = batch.time;

                let frontier = self.progress.iter().min().unwrap();
                while let Some((time, _)) = self.stash.first_key_value() && time < frontier {
                    let (time, updates) = self.stash.pop_first().unwrap();
                    yield Batch { time, updates };
                }
            }
        }
        .boxed()
    }

    pub fn snapshot(self) -> BoxStream<'static, anyhow::Result<Batch>> {
        let stream = self.into_stream();
        wrapper::Snapshot::new(stream).boxed()
    }

    pub fn listen(self, duration: Duration) -> BoxStream<'static, anyhow::Result<Batch>> {
        let stream = self.into_stream();
        wrapper::Listen::new(stream, duration).boxed()
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
