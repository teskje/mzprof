use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures::{Stream, StreamExt};

use super::{Batch, Data};

pub(super) struct Snapshot<S> {
    inner: S,
    as_of: Option<Duration>,
    finished: bool,
}

impl<S> Snapshot<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            as_of: None,
            finished: false,
        }
    }

    fn filter_batch(&mut self, batch: &mut Batch) {
        let as_of = *self.as_of.get_or_insert(batch.time);
        batch.updates.retain(|update| update.time <= as_of);

        if batch.time > as_of {
            self.finished = true;
        }
    }
}

impl<S: Stream<Item = anyhow::Result<Batch>> + Unpin> Stream for Snapshot<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut result = ready!(self.inner.poll_next_unpin(cx));

        if let Some(Ok(batch)) = &mut result {
            self.filter_batch(batch);
        }

        Poll::Ready(result)
    }
}

pub(super) struct Listen<S> {
    inner: S,
    as_of: Option<Duration>,
    duration: Duration,
    finished: bool,
}

impl<S> Listen<S> {
    pub fn new(inner: S, duration: Duration) -> Self {
        Self {
            inner,
            as_of: None,
            duration,
            finished: false,
        }
    }

    fn filter_batch(&mut self, batch: &mut Batch) {
        let as_of = *self.as_of.get_or_insert(batch.time);
        let up_to = as_of + self.duration;

        // Skip the snapshot for incremental profile types only.
        batch.updates.retain(|update| match &update.data {
            Data::Operator(..) | Data::Size(..) => update.time <= up_to,
            Data::Elapsed(..) => update.time > as_of && update.time <= up_to,
        });

        if batch.time > up_to {
            self.finished = true;
        }
    }
}

impl<S: Stream<Item = anyhow::Result<Batch>> + Unpin> Stream for Listen<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut result = ready!(self.inner.poll_next_unpin(cx));

        if let Some(Ok(batch)) = &mut result {
            self.filter_batch(batch);
        }

        Poll::Ready(result)
    }
}
