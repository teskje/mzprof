use std::collections::BTreeMap;
use std::time::Duration;

use crate::collect::{Batch, Data};
use crate::pprof::StringTable;
use crate::pprof::profile as pp;
use crate::types::{Address, OpId, OpInfo, WorkerId};

pub struct Aggregator {
    start: Option<Duration>,
    operators: BTreeMap<OpId, OpInfo>,
    elapsed: BTreeMap<(OpId, WorkerId), Duration>,
    sizes: BTreeMap<(OpId, WorkerId), i64>,
    capacities: BTreeMap<(OpId, WorkerId), i64>,
    records: BTreeMap<(OpId, WorkerId), i64>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            start: None,
            operators: BTreeMap::new(),
            elapsed: BTreeMap::new(),
            sizes: BTreeMap::new(),
            capacities: BTreeMap::new(),
            records: BTreeMap::new(),
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
                Data::Capacity(id, worker) => self.update_capacity(id, worker, diff),
                Data::Records(id, worker) => self.update_records(id, worker, diff),
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

    fn update_capacity(&mut self, id: OpId, worker: WorkerId, diff: i64) {
        self.capacities
            .entry((id, worker))
            .and_modify(|x| *x += diff)
            .or_insert(diff);
    }

    fn update_records(&mut self, id: OpId, worker: WorkerId, diff: i64) {
        self.records
            .entry((id, worker))
            .and_modify(|x| *x += diff)
            .or_insert(diff);
    }

    pub fn build_pprof(&self) -> pp::Profile {
        let mut builder = ProfileBuilder::new();

        if let Some(time) = self.start {
            builder.set_time(time);
        }

        for (id, info) in &self.operators {
            builder.add_operator(*id, info);
        }

        if !self.elapsed.is_empty() {
            let ops_by_address: BTreeMap<_, _> = self
                .operators
                .iter()
                .map(|(id, op)| (&op.address, *id))
                .collect();

            let mut elapsed_ns: BTreeMap<_, _> = self
                .elapsed
                .iter()
                .map(|(key, duration)| (*key, duration_to_nanos(*duration)))
                .collect();

            // Elapsed times are cumulative, i.e. each node includes the elapsed times of its
            // children. We need to make them non-cumulative, to match pprof's expectations.
            for (&(id, worker), &duration) in self.elapsed.iter().rev() {
                let parent_ns = self
                    .operators
                    .get(&id)
                    .and_then(|op| op.address.parent())
                    .and_then(|parent_addr| ops_by_address.get(&parent_addr))
                    .and_then(|parent_id| elapsed_ns.get_mut(&(*parent_id, worker)));

                if let Some(parent_ns) = parent_ns {
                    let nanos = duration_to_nanos(duration);
                    *parent_ns = parent_ns.saturating_sub(nanos);
                }
            }

            builder.add_samples("time", "nanoseconds", &elapsed_ns);
        }

        if !self.sizes.is_empty() {
            builder.add_samples("size", "bytes", &self.sizes);
        }
        if !self.capacities.is_empty() {
            builder.add_samples("capacity", "bytes", &self.capacities);
        }
        if !self.records.is_empty() {
            builder.add_samples("records", "count", &self.records);
        }

        builder.build()
    }
}

/// Convert the given duration into an `i64` nanoseconds value.
///
/// # Panics
///
/// Panics if the amount of nanoseconds doesn't fit in an `i64`, i.e. if the duration is longer
/// than 292 years.
fn duration_to_nanos(duration: Duration) -> i64 {
    duration.as_nanos().try_into().unwrap()
}

struct ProfileBuilder<'a> {
    string_table: StringTable,
    locations: BTreeMap<OpId, pp::Location>,
    functions: BTreeMap<OpId, pp::Function>,
    sample_types: Vec<pp::ValueType>,
    samples: BTreeMap<(OpId, WorkerId), pp::Sample>,
    op_addrs_by_id: BTreeMap<OpId, &'a Address>,
    op_ids_by_addr: BTreeMap<&'a Address, OpId>,
    time: Option<Duration>,
}

impl<'a> ProfileBuilder<'a> {
    fn new() -> Self {
        Self {
            string_table: StringTable::new(),
            locations: BTreeMap::new(),
            functions: BTreeMap::new(),
            sample_types: Vec::new(),
            samples: BTreeMap::new(),
            op_addrs_by_id: BTreeMap::new(),
            op_ids_by_addr: BTreeMap::new(),
            time: None,
        }
    }

    fn add_string(&mut self, s: &str) -> i64 {
        self.string_table.insert(s)
    }

    fn set_time(&mut self, time: Duration) {
        self.time = Some(time);
    }

    fn add_operator(&mut self, id: OpId, info: &'a OpInfo) {
        self.add_location(id, &info.name);
        self.op_addrs_by_id.insert(id, &info.address);
        self.op_ids_by_addr.insert(&info.address, id);
    }

    fn add_location(&mut self, id: OpId, name: &str) {
        let function = pp::Function {
            id,
            name: self.add_string(name),
            ..Default::default()
        };
        let location = pp::Location {
            id,
            address: id,
            line: vec![pp::Line {
                function_id: id,
                ..Default::default()
            }],
            ..Default::default()
        };

        self.functions.insert(id, function);
        self.locations.insert(id, location);
    }

    fn add_samples(&mut self, type_: &str, unit: &str, samples: &BTreeMap<(OpId, WorkerId), i64>) {
        let sample_type = pp::ValueType {
            type_: self.add_string(type_),
            unit: self.add_string(unit),
            ..Default::default()
        };

        self.sample_types.push(sample_type);
        for sample in self.samples.values_mut() {
            sample.value.push(0);
        }

        let len = self.sample_types.len();

        for (&key, &value) in samples {
            let (id, worker) = key;
            if !self.samples.contains_key(&key) {
                let stack = self.build_operator_stack(id);
                let sample = pp::Sample {
                    location_id: stack,
                    value: vec![0; len],
                    label: vec![pp::Label {
                        key: self.add_string("worker"),
                        str: self.add_string(&worker.to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                self.samples.insert(key, sample);
            }

            let sample = self.samples.get_mut(&key).unwrap();
            sample.value[len - 1] = value;
        }
    }

    fn build_operator_stack(&mut self, id: OpId) -> Vec<OpId> {
        let mut stack = vec![id];

        if let Some(addr) = self.op_addrs_by_id.get(&id) {
            for addr in addr.ancestors() {
                stack.push(self.op_ids_by_addr[&addr]);
            }
        } else if !self.locations.contains_key(&id) {
            self.add_location(id, "<unknown>");
        }

        stack
    }

    fn build(self) -> pp::Profile {
        let mut prof = pp::Profile::new();

        if let Some(time) = self.time {
            prof.time_nanos = duration_to_nanos(time);
        }

        prof.function = self.functions.into_values().collect();
        prof.location = self.locations.into_values().collect();
        prof.sample_type = self.sample_types;
        prof.sample = self.samples.into_values().collect();

        prof.string_table = self.string_table.finish();
        prof
    }
}
