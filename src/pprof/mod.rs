use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use flate2::Compression;
use flate2::write::GzEncoder;
use protobuf::Message;

use self::profile as pp;

mod generated {
    #![allow(clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}
pub use generated::*;

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

/// Write a pprof profile to a file path.
pub fn write_file(prof: &pp::Profile, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);

    let mut gz = GzEncoder::new(writer, Compression::default());
    prof.write_to_writer(&mut gz)?;
    gz.finish()?;

    Ok(())
}
