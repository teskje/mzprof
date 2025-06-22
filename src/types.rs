use std::time::Duration;

pub type OpId = u64;
pub type WorkerId = u64;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpInfo {
    pub id: OpId,
    pub name: String,
    pub address: Address,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Address(pub Box<[u64]>);

impl Address {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Address> {
        (1..self.0.len())
            .map(|i| Address(self.0[0..i].into()))
            .rev()
    }

    pub fn parent(&self) -> Option<Address> {
        self.ancestors().next()
    }
}

#[derive(Clone, Debug)]
pub struct Update<D> {
    pub data: D,
    pub time: Duration,
    pub diff: i64,
}

#[derive(Debug)]
pub struct Batch<D> {
    pub time: Duration,
    pub updates: Vec<Update<D>>,
}
