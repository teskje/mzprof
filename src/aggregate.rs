use std::collections::BTreeMap;
use std::time::Duration;

use crate::pprof::StringTable;
use crate::pprof::profile as pp;
use crate::types::{Batch, OpId, OpInfo, WorkerId};

pub struct ElapsedAggregator {
    start: Option<Duration>,
    operators: BTreeMap<OpId, OpInfo>,
    elapsed: BTreeMap<(OpId, WorkerId), Duration>,
}

impl ElapsedAggregator {
    pub fn new() -> Self {
        Self {
            start: None,
            operators: BTreeMap::default(),
            elapsed: BTreeMap::default(),
        }
    }

    pub fn update(&mut self, batch: Batch<(OpInfo, WorkerId)>) {
        if self.start.is_none() {
            self.start = Some(batch.time);
        }

        for update in batch.updates {
            let Ok(elapsed_ns) = u64::try_from(update.diff) else {
                continue;
            };

            let (op, worker) = update.data;
            let elapsed = Duration::from_nanos(elapsed_ns);

            self.elapsed
                .entry((op.id, worker))
                .and_modify(|x| *x += elapsed)
                .or_insert(elapsed);
            self.operators.insert(op.id, op);
        }
    }

    pub fn build_pprof(&self) -> pp::Profile {
        let ops_by_address: BTreeMap<_, _> = self
            .operators
            .iter()
            .map(|(id, op)| (&op.address, *id))
            .collect();

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

        let mut prof = pp::Profile::new();
        let mut ss = StringTable::new();

        prof.sample_type = vec![pp::ValueType {
            type_: ss.insert("time"),
            unit: ss.insert("nanoseconds"),
            ..Default::default()
        }];

        let start = self.start.unwrap_or(Duration::ZERO);
        prof.time_nanos = start.as_nanos().try_into().unwrap();

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

        for ((id, worker), duration) in elapsed {
            let nanos = duration.as_nanos().try_into().unwrap();
            prof.sample.push(pp::Sample {
                location_id: op_stacks[&id].clone(),
                value: vec![nanos],
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

pub struct SizeAggregator {
    start: Option<Duration>,
    operators: BTreeMap<OpId, OpInfo>,
    sizes: BTreeMap<(OpId, WorkerId), i64>,
}

impl SizeAggregator {
    pub fn new() -> Self {
        Self {
            start: None,
            operators: BTreeMap::default(),
            sizes: BTreeMap::default(),
        }
    }

    pub fn update(&mut self, batch: Batch<(OpInfo, Option<WorkerId>)>) {
        if self.start.is_none() {
            self.start = Some(batch.time);
        }

        for update in batch.updates {
            let (op, worker) = update.data;
            let size_diff = update.diff;

            // If the worker is `None` it means the operator has no size.
            // We still need to record it because its children may have a size.
            if let Some(worker) = worker {
                self.sizes
                    .entry((op.id, worker))
                    .and_modify(|x| *x += size_diff)
                    .or_insert(size_diff);
            }
            self.operators.insert(op.id, op);
        }
    }

    pub fn build_pprof(&self) -> pp::Profile {
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

        let mut prof = pp::Profile::new();
        let mut ss = StringTable::new();

        prof.sample_type = vec![pp::ValueType {
            type_: ss.insert("size"),
            unit: ss.insert("bytes"),
            ..Default::default()
        }];

        let start = self.start.unwrap_or(Duration::ZERO);
        prof.time_nanos = start.as_nanos().try_into().unwrap();

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

        for ((id, worker), &size) in &self.sizes {
            prof.sample.push(pp::Sample {
                location_id: op_stacks[id].clone(),
                value: vec![size],
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
