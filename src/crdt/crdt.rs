use core::error::Error;

use exn::Result;
use itc::{EventTree, Stamp};


#[derive(Debug, PartialEq)]
pub struct OperationDelta<T> {
    pub base_history: EventTree,
    pub target_history: EventTree,
    pub operations: T,
}

impl<T> OperationDelta<T> {
    pub fn new(
        base_history: EventTree,
        target_history: EventTree,
        operations: T,
    ) -> Self {
        Self { base_history, target_history, operations }
    }

    pub fn is_applicable_to(&self, history: &EventTree) -> bool {
        history.dominates(&self.base_history)
    }
}


#[derive(Debug, Clone)]
pub struct Mutation<T, U> {
    pub add: T,
    pub remove: U,
}

impl<T, U> Mutation<T, U> {
    pub fn new(add: T, remove: U) -> Self {
        Self { add, remove }
    }

    pub fn destruct(self) -> (T, U) {
        (self.add, self.remove)
    }
}


#[derive(Debug, PartialEq)]
pub struct ReplicaState<T> {
    pub stamp: Stamp,
    pub delta: OperationDelta<T>,
}

impl<T> ReplicaState<T> {
    pub fn new(
        stamp: Stamp,
        delta: OperationDelta<T>,
    ) -> Self {
        Self { stamp, delta }
    }
}


pub trait Crdt<T, E>
where E: Error + Send + Sync
{
    fn get_delta_since(&mut self, history: EventTree) -> Result<OperationDelta<T>, E>;
    fn apply_delta(&mut self, delta: OperationDelta<T>) -> Result<Mutation<T, T>, E>;
    fn fork(&mut self) -> Result<ReplicaState<T>, E>;
    fn join(&mut self, replica_state: ReplicaState<T>) -> Result<Mutation<T, T>, E>;
}
