use core::error::Error;
use core::future::Future;
use core::pin::Pin;

use exn::Result;
use itc::{EventTree, Stamp};


pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;


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
where E: Error + Send + Sync + 'static
{
    type EmtpyFuture<'a>: Future<Output = Result<(), E>> + 'a
    where
        Self: 'a;

    type OperationDeltaFuture<'a>: Future<Output = Result<OperationDelta<T>, E>> + 'a
    where
        Self: 'a;

    type ReplicaStateFuture<'a>: Future<Output = Result<ReplicaState<T>, E>> + 'a
    where
        Self: 'a;

    fn get_delta_since<'a>(&'a mut self, history: EventTree) -> Self::OperationDeltaFuture<'a>;
    fn apply_delta<'a>(&'a mut self, delta: OperationDelta<T>) -> Self::EmtpyFuture<'a>;
    fn fork<'a>(&'a mut self) -> Self::ReplicaStateFuture<'a>;
    fn join<'a>(&'a mut self, replica_state: ReplicaState<T>) -> Self::EmtpyFuture<'a>;
}
