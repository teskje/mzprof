use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::collect::{Batch, Data};
use crate::pprof::StringTable;
use crate::pprof::profile as pp;
use crate::types::{OpId, OpInfo, WorkerId};

pub struct Aggregator {
    start: Option<Duration>,
    operators: BTreeMap<OpId, OpInfo>,
    elapsed: BTreeMap<(OpId, WorkerId), Duration>,
    sizes: BTreeMap<(OpId, WorkerId), i64>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            start: None,
            operators: BTreeMap::new(),
            elapsed: BTreeMap::new(),
            sizes: BTreeMap::new(),
        }
    }

    pub fn update(&mut self, batch: Batch) {
        if self.start.is_none() {
            self.start = Some(batch.time);
        }

        for update in batch.updates {
            let diff = update.diff;
            match update.data {
                Data::Operator(id, info) => self.update_operator(id, info, diff),
                Data::Elapsed(id, worker) => self.update_elapsed(id, worker, diff),
                Data::Size(id, worker) => self.update_size(id, worker, diff),
            }
        }
    }

    fn update_operator(&mut self, id: OpId, info: OpInfo, diff: i64) {
        if diff > 0 {
            self.operators.insert(id, info);
        }
    }

    fn update_elapsed(&mut self, id: OpId, worker: WorkerId, diff: i64) {
        if let Ok(nanos) = u64::try_from(diff) {
            let elapsed = Duration::from_nanos(nanos);
            self.elapsed
                .entry((id, worker))
                .and_modify(|x| *x += elapsed)
                .or_insert(elapsed);
        }
    }

    fn update_size(&mut self, id: OpId, worker: WorkerId, diff: i64) {
        self.sizes
            .entry((id, worker))
            .and_modify(|x| *x += diff)
            .or_insert(diff);
    }

    pub fn build_pprof(&self) -> pp::Profile {
        // TODO clean this up

        let mut prof = pp::Profile::new();
        let mut ss = StringTable::new();

        if !self.elapsed.is_empty() {
            prof.sample_type.push(pp::ValueType {
                type_: ss.insert("time"),
                unit: ss.insert("nanoseconds"),
                ..Default::default()
            });
        }
        if !self.sizes.is_empty() {
            prof.sample_type.push(pp::ValueType {
                type_: ss.insert("size"),
                unit: ss.insert("bytes"),
                ..Default::default()
            });
        }

        let start = self.start.unwrap_or(Duration::ZERO);
        prof.time_nanos = start.as_nanos().try_into().unwrap();

        let ops_by_address: BTreeMap<_, _> = self
            .operators
            .iter()
            .map(|(id, op)| (&op.address, *id))
            .collect();

        // Build call stack for each operator.
        let mut op_stacks = BTreeMap::new();
        for (&id, op) in &self.operators {
            let mut stack = Vec::with_capacity(op.address.len());
            stack.push(id);
            for addr in op.address.ancestors() {
                stack.push(ops_by_address[&addr]);
            }
            op_stacks.insert(id, stack);
        }

        for (&id, op) in &self.operators {
            prof.function.push(pp::Function {
                id,
                name: ss.insert(&op.name),
                ..Default::default()
            });
            prof.location.push(pp::Location {
                id,
                address: id,
                line: vec![pp::Line {
                    function_id: id,
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        // It's possible to get samples for unknown operators. We insert dummy locations for those.
        let mut unknown_locations = BTreeSet::new();

        // Elapsed times are cumulative, i.e. each node includes the elapsed times of its children.
        // We need to make them non-cumulative, to match pprof's expectations.
        let mut elapsed = self.elapsed.clone();
        for (&(id, worker), &duration) in self.elapsed.iter().rev() {
            let op = &self.operators[&id];
            let Some(parent_addr) = op.address.parent() else {
                continue;
            };

            let Some(&parent_id) = ops_by_address.get(&&parent_addr) else {
                panic!("parent operator missing");
            };
            let Some(parent_elapsed) = elapsed.get_mut(&(parent_id, worker)) else {
                panic!("parent elapsed missing");
            };
            *parent_elapsed = parent_elapsed.saturating_sub(duration);
        }

        let mut samples = BTreeMap::new();
        for ((id, worker), duration) in elapsed {
            let nanos = duration.as_nanos().try_into().unwrap();
            if self.sizes.is_empty() {
                samples.insert((id, worker), vec![nanos]);
            } else {
                samples.insert((id, worker), vec![nanos, 0]);
            }
        }
        for (&(id, worker), &size) in &self.sizes {
            if self.elapsed.is_empty() {
                samples.insert((id, worker), vec![size]);
            } else {
                let entry = samples.entry((id, worker)).or_insert(vec![0, 0]);
                entry[1] = size;
            }
        }

        for ((id, worker), values) in samples {
            let stack = match op_stacks.get(&id) {
                Some(stack) => stack.clone(),
                None => {
                    if !unknown_locations.contains(&id) {
                        prof.function.push(pp::Function {
                            id,
                            name: ss.insert("???"),
                            ..Default::default()
                        });
                        prof.location.push(pp::Location {
                            id,
                            address: id,
                            line: vec![pp::Line {
                                function_id: id,
                                ..Default::default()
                            }],
                            ..Default::default()
                        });
                        unknown_locations.insert(id);
                    }
                    vec![id]
                }
            };

            prof.sample.push(pp::Sample {
                location_id: stack,
                value: values,
                label: vec![pp::Label {
                    key: ss.insert("worker"),
                    str: ss.insert(&worker.to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        prof.string_table = ss.finish();
        prof
    }
}
