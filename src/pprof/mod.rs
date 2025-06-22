use std::collections::BTreeMap;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

/// Helper struct to simplify building a `string_table`.
#[derive(Default)]
pub struct StringTable(BTreeMap<String, i64>);

impl StringTable {
    pub fn new() -> Self {
        // Element 0 must always be the empty string.
        let inner = [(String::new(), 0)].into();
        Self(inner)
    }

    pub fn insert(&mut self, s: &str) -> i64 {
        if let Some(idx) = self.0.get(s) {
            *idx
        } else {
            let idx = i64::try_from(self.0.len()).expect("must fit");
            self.0.insert(s.into(), idx);
            idx
        }
    }

    pub fn finish(self) -> Vec<String> {
        let mut vec: Vec<_> = self.0.into_iter().collect();
        vec.sort_by_key(|(_, idx)| *idx);
        vec.into_iter().map(|(s, _)| s).collect()
    }
}
