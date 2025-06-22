mod subscribe;
mod wrapper;

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use tokio::sync::mpsc;

use crate::collect::subscribe::Subscribe;
use crate::types::{Batch, OpInfo, WorkerId};

pub struct Collector {
    connect_options: PgConnectOptions,
    event_tx: mpsc::UnboundedSender<Event>,
    event_rx: mpsc::UnboundedReceiver<Event>,
}

impl Collector {
    pub fn new(sql_url: &str, cluster: &str, replica: &str) -> anyhow::Result<Self> {
        let connect_options = sql_url
            .parse::<PgConnectOptions>()?
            .application_name("mzprof")
            .options([("cluster", cluster), ("cluster_replica", replica)]);

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Ok(Self {
            connect_options,
            event_tx,
            event_rx,
        })
    }

    async fn connect(&self) -> anyhow::Result<PgConnection> {
        let conn = PgConnection::connect_with(&self.connect_options).await?;
        Ok(conn)
    }

    pub async fn subscribe_elapsed(&self) -> anyhow::Result<()> {
        self.subscribe(subscribe::Elapsed).await
    }

    async fn subscribe<S>(&self, subs: S) -> anyhow::Result<()>
    where
        S: Subscribe,
    {
        let conn = self.connect().await?;
        let tx = self.event_tx.clone();

        tokio::spawn(async move {
            if let Err(error) = subscribe::run(subs, conn, tx.clone()).await {
                let _ = tx.send(Event::Error(error));
            }
        });

        Ok(())
    }

    pub fn snapshot(self) -> Pin<Box<dyn Stream<Item = Event>>> {
        wrapper::Snapshot::new(self).boxed()
    }

    pub fn listen(self, duration: Duration) -> Pin<Box<dyn Stream<Item = Event>>> {
        wrapper::Listen::new(self, duration).boxed()
    }
}

impl Stream for Collector {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.event_rx.poll_recv(cx)
    }
}

#[derive(Debug)]
pub enum Event {
    Elapsed(Batch<(OpInfo, WorkerId)>),
    Error(anyhow::Error),
}
