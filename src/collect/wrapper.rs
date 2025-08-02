use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures::{Stream, StreamExt};

use crate::types::Batch;

use super::{Collector, Event};

pub(super) struct Snapshot {
    collector: Collector,
    as_of: Option<Duration>,
    finished: bool,
}

impl Snapshot {
    pub fn new(collector: Collector) -> Self {
        Self {
            collector,
            as_of: None,
            finished: false,
        }
    }

    fn filter_batch<D>(&mut self, batch: &mut Batch<D>) {
        let as_of = *self.as_of.get_or_insert(batch.time);
        batch.updates.retain(|update| update.time <= as_of);

        if batch.time > as_of {
            self.finished = true;
        }
    }
}

impl Stream for Snapshot {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut event = ready!(self.collector.poll_next_unpin(cx));

        match &mut event {
            Some(Event::Elapsed(batch)) => self.filter_batch(batch),
            Some(Event::Size(batch)) => self.filter_batch(batch),
            Some(Event::Error(_)) | None => return Poll::Ready(event),
        }

        Poll::Ready(event)
    }
}

pub(super) struct Listen {
    collector: Collector,
    as_of: Option<Duration>,
    duration: Duration,
    finished: bool,
}

impl Listen {
    pub fn new(collector: Collector, duration: Duration) -> Self {
        Self {
            collector,
            as_of: None,
            duration,
            finished: false,
        }
    }

    fn filter_batch<D>(&mut self, batch: &mut Batch<D>) {
        let as_of = *self.as_of.get_or_insert(batch.time);
        let up_to = as_of + self.duration;
        batch
            .updates
            .retain(|update| update.time > as_of && update.time <= up_to);

        if batch.time > up_to {
            self.finished = true;
        }
    }
}

impl Stream for Listen {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut event = ready!(self.collector.poll_next_unpin(cx));

        match &mut event {
            Some(Event::Elapsed(batch)) => self.filter_batch(batch),
            Some(Event::Size(batch)) => self.filter_batch(batch),
            Some(Event::Error(_)) | None => return Poll::Ready(event),
        }

        Poll::Ready(event)
    }
}
