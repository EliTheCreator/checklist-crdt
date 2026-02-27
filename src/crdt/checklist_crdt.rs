use std::cmp::Ordering;

use exn::{bail, Exn, Result, ResultExt};
use itc::{EventTree, IdTree, IntervalTreeClock, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::crdt::crdt::{BoxedFuture, Crdt, OperationDelta, ReplicaState};
use crate::crdt::crdt_error::CrdtError;
use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::{StorageError, ErrorKind};
use crate::persistence::storage::Store;


macro_rules! try_abort {
    ($this:expr, $operation:expr, $message:expr) => {{
        match $operation.await {
            Ok(result) => Ok(result),
            Err(e) => Err($this.abort_transaction(e, $message).await),
        }
    }};
}


macro_rules! transaction {
    ($this:expr, $stamp:expr, $body:expr) => {{
        $this.storage
            .start_transaction()
            .await
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        let result = (async || -> Result<_, _> { $body.await })().await?;

        let _ = try_abort!(
            $this,
            $this.storage.save_stamp($stamp.clone()),
            "crdt unable to save stamp"
        )?;

        let _ = try_abort!($this,
            $this.storage.commit_transaction(),
            "crdt unable commit transaction"
        )?;

        $this.itc_stamp = $stamp;

        exn::Ok(result)
    }};
}


#[derive(Debug, PartialEq)]
pub struct ChecklistOperations {
    head_operations: Vec<HeadOperation>,
    item_operations: Vec<ItemOperation>,
}

impl ChecklistOperations {
    pub fn new(
        head_operations: Vec<HeadOperation>,
        item_operations: Vec<ItemOperation>,
    ) -> Self {
        Self { head_operations, item_operations }
    }
}


pub struct ChecklistCrdt<S: Store> {
    storage: S,
    itc_stamp: Stamp
}

impl<S: Store> ChecklistCrdt<S> {
    pub async fn new(mut storage: S) -> Result<Self, CrdtError> {
        let stamp = match storage.load_stamp().await {
            Ok(s) => s,
            Err(e) if e.kind == ErrorKind::StampNone =>{
                let stamp = Stamp::seed();
                storage.save_stamp(stamp.clone()).await
                    .or_raise(|| CrdtError::fatal("failed to save new stamp"))?;
                stamp
            },
            Err(_) => bail!(CrdtError::fatal("failed to load stamp")),
        };

        Ok(ChecklistCrdt {
            storage: storage,
            itc_stamp: stamp,
        })}

    pub async fn new_from(
        storage: S,
        replica_state: ReplicaState<ChecklistOperations>
    ) -> Result<Self, CrdtError> {
        let mut crdt = ChecklistCrdt {
            storage: storage,
            itc_stamp: Stamp::new(IdTree::zero(), EventTree::zero()),
        };

        crdt.join(replica_state).await
            .or_raise(|| CrdtError::fatal("failed to join replica"))?;

        Ok(crdt)
    }

    async fn abort_transaction(
        &mut self,
        e1: Exn<StorageError>,
        text: &str,
    ) -> Exn<CrdtError> {
        let success_text = format!("{text}. all operations have been reverted");
        let failure_text = format!("{text}. unable to reverse all operations. crdt is in inconsistent state");
        match self.storage.abort_transaction().await {
            Ok(_) => {
                e1.raise(CrdtError::recovered(success_text))
            },
            Err(e2) => {
                Exn::raise_all(CrdtError::fatal(failure_text), vec![e1, e2])
            },
        }
    }

    async fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), CrdtError> {
        let associated_operations = try_abort!(
            self,
            self.storage.load_all_associated_head_operations(operation.head_id().clone()),
            "crdt unable to load associated head operations"
        )?;

        let mut obsolete_ops = Vec::new();
        for assoc_op in associated_operations.iter() {
            use HeadOperation::*;
            match (&assoc_op, &operation) {
                (Creation { .. }, Creation { .. })
                | (NameUpdate { .. }, NameUpdate { .. })
                | (DescriptionUpdate { .. }, DescriptionUpdate { .. })
                | (CompletedUpdate { .. }, CompletedUpdate { .. })
                | (Deletion { .. }, Deletion { .. }) => {
                    match assoc_op.history().partial_cmp(operation.history()) {
                        Some(Ordering::Less) => obsolete_ops.push(assoc_op),
                        None if assoc_op.id() < operation.id() => obsolete_ops.push(assoc_op),
                        _ => return Ok(()),
                    }
                },
                _ => (),
            }
        }

        for obsolete_op in obsolete_ops {
            self.erase_head_operation(obsolete_op.id()).await?;
        }

        try_abort!(
            self,
            self.storage.save_head_operation(operation),
            "crdt unable to store head operation"
        )
    }

    async fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, CrdtError> {
        try_abort!(
            self,
            self.storage.erase_head_operation(id.clone()),
            "crdt unable to erase head operation"
        )
    }

    pub async fn add_head(
        &mut self,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let operation = HeadOperation::Creation {
            id: id.clone(),
            history: stamp.history(),
            template_id: template_id,
            name: name,
            description: description,
        };

        let _: Result<(), CrdtError> = transaction!(self, stamp, {
            self.save_head_operation(operation)
        });
        Ok(id)
    }

    pub async fn update_head_name(
        &mut self,
        head_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::NameUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            history: stamp.history(),
            name: name,
        };

        transaction!(self, stamp, self.save_head_operation(operation))
    }

    pub async fn update_head_description(
        &mut self,
        head_id: &Uuid,
        description: Option<String>,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::DescriptionUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            history: stamp.history(),
            description: description,
        };

        transaction!(self, stamp, self.save_head_operation(operation))
    }

    pub async fn update_head_completed(
        &mut self,
        head_id: &Uuid,
        completed: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::CompletedUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            history: stamp.history(),
            completed: completed,
        };

        transaction!(self, stamp, self.save_head_operation(operation))
    }

    pub async fn delete_head(
        &mut self,
        head_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::Deletion {
            id: Uuid::now_v7(),
            history: stamp.history(),
            head_id: head_id.clone(),
        };

        transaction!(self, stamp, self.save_head_operation(operation))
    }

    pub async fn get_heads(&mut self) -> Result<Vec<HeadOperation>, CrdtError> {
        self.storage.load_all_head_operations().await
            .or_raise(|| CrdtError::recovered("crdt unable to load all head operations"))
    }

    async fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), CrdtError> {
        let associated_operations = try_abort!(
            self,
            self.storage.load_all_associated_item_operations(operation.item_id().clone()),
            "crdt unable to load associated item operations"
        )?;

        let mut obsolete_ops = Vec::new();
        for assoc_op in associated_operations.iter() {
            use ItemOperation::*;
            match (&assoc_op, &operation) {
                (Creation { .. }, Creation { .. })
                | (NameUpdate { .. }, NameUpdate { .. })
                | (PositionUpdate { .. }, PositionUpdate { .. })
                | (CheckedUpdate { .. }, CheckedUpdate { .. })
                | (Deletion { .. }, Deletion { .. }) => {
                    match assoc_op.history().partial_cmp(operation.history()) {
                        Some(Ordering::Less) => obsolete_ops.push(assoc_op),
                        None if assoc_op.id() < operation.id() => obsolete_ops.push(assoc_op),
                        _ => return Ok(()),
                    }
                },
                _ => (),
            }
        }

        for obsolete_op in obsolete_ops {
            self.erase_item_operation(obsolete_op.id()).await?;
        }

        try_abort!(
            self,
            self.storage.save_item_operation(operation),
            "crdt unable to store item operation"
        )
    }

    async fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, CrdtError> {
        try_abort!(
            self,
            self.storage.erase_item_operation(id.clone()),
            "crdt unable to erase item operation"
        )
    }

    pub async fn add_item(
        &mut self,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let operation = ItemOperation::Creation {
            id: id.clone(),
            history: stamp.history(),
            head_id: head_id,
            name: name,
            position: position,
        };

        let _: Result<(), CrdtError> = transaction!(self, stamp, self.save_item_operation(operation));
        Ok(id)
    }

    pub async fn update_item_name(
        &mut self,
        item_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::NameUpdate {
            id: Uuid::now_v7(),
            history: stamp.history(),
            item_id: item_id.clone(),
            name: name,
        };

        transaction!(self, stamp, self.save_item_operation(operation))
    }

    pub async fn update_item_position(
        &mut self,
        item_id: &Uuid,
        position: FractionalIndex,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::PositionUpdate {
            id: Uuid::now_v7(),
            history: stamp.history(),
            item_id: item_id.clone(),
            position: position,
        };

        transaction!(self, stamp, self.save_item_operation(operation))
    }

    pub async fn update_item_checked(
        &mut self,
        item_id: &Uuid,
        checked: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::CheckedUpdate {
            id: Uuid::now_v7(),
            history: stamp.history(),
            item_id: item_id.clone(),
            checked: checked,
        };

        transaction!(self, stamp, self.save_item_operation(operation))
    }

    pub async fn delete_item(
        &mut self,
        item_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation: ItemOperation = ItemOperation::Deletion {
            id: Uuid::now_v7(),
            history: stamp.history(),
            item_id: item_id.clone(),
        };

        transaction!(self, stamp, self.save_item_operation(operation))
    }

    pub async fn get_items(&mut self) -> Result<Vec<ItemOperation>, CrdtError> {
        self.storage.load_all_item_operations().await
            .or_raise(|| CrdtError::recovered("crdt unable to load all item operations"))
    }
}

impl<S: Store> Crdt<ChecklistOperations, CrdtError> for ChecklistCrdt<S> {
    type EmtpyFuture<'a> = BoxedFuture<'a, Result<(), CrdtError>> where Self: 'a;
    type OperationDeltaFuture<'a> = BoxedFuture<'a, Result<OperationDelta<ChecklistOperations>, CrdtError>> where Self: 'a;
    type ReplicaStateFuture<'a> = BoxedFuture<'a, Result<ReplicaState<ChecklistOperations>, CrdtError>> where Self: 'a;

    fn get_delta_since<'a>(&'a mut self, history: EventTree) -> Self::OperationDeltaFuture<'a> {
    Box::pin(async move {
        let mut head_operations = self.get_heads().await?;
        head_operations.retain(|head| {
            match history.partial_cmp(head.history()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let mut item_operations = self.get_items().await?;
        item_operations.retain(|item| {
            match history.partial_cmp(item.history()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let local_history = self.itc_stamp.history();

        let operations = ChecklistOperations::new(head_operations, item_operations);
        Ok(OperationDelta::new(history, local_history, operations))
    })}

    fn apply_delta<'a>(&'a mut self, delta: OperationDelta<ChecklistOperations>) -> Self::EmtpyFuture<'a> {
    Box::pin(async move {
        if !delta.is_applicable_to(&self.itc_stamp.history()) {
            bail!(CrdtError::causal_gap(concat!(
                "causal gap between local and base history deteceted. this may lead to lost ",
                "operations and to an invalid crdt state"
            )))
        }

        let replica_stamp = Stamp::new(IdTree::zero(), delta.target_history.clone());
        let replica_state = ReplicaState::new(replica_stamp, delta);
        self.join(replica_state).await
    })}

    fn fork<'a>(&'a mut self) -> Self::ReplicaStateFuture<'a> {
    Box::pin(async move {
        let (stamp, replica_stamp) = self.itc_stamp.fork();
        let head_operations = self.get_heads().await?;
        let item_operations = self.get_items().await?;

        transaction!(self, stamp, async { Ok(()) })?;
        let operations = ChecklistOperations::new(head_operations, item_operations);
        let delta = OperationDelta::new(
            EventTree::zero(),
            self.itc_stamp.history().clone(),
            operations
        );
        Ok(ReplicaState::new(replica_stamp, delta))
    })}

    fn join<'a>(&'a mut self, replica_state: ReplicaState<ChecklistOperations>) -> Self::EmtpyFuture<'a> {
    Box::pin(async move {
        let stamp = self.itc_stamp.join(&replica_state.stamp).event();

        transaction!(self, stamp, async {
            for operation in replica_state.delta.operations.head_operations {
                self.save_head_operation(operation).await?;
            }

            for operation in replica_state.delta.operations.item_operations {
                self.save_item_operation(operation).await?;
            }

            Ok(())
        })
    })}
}


#[cfg(test)]
mod test {
    // use crate::persistence::storage::{FileStorage, InMemoryStorage};

    // use super::*;

    // #[test]
    // fn add_items() {
    //     use crate::persistence::storage::FileStorage;

    //     let stamp_path = "./stamp.txt";
    //     let head_log_path = "./head_log.txt";
    //     let item_log_path = "./item_log.txt";
    //     let file_store = FileStorage::new(
    //         stamp_path,
    //         head_log_path,
    //         item_log_path,
    //     ).unwrap();


    //     let mut crdt = ChecklistCrdt::new(file_store).unwrap();
    //     let index = FractionalIndex::default();
    //     crdt.add_item(
    //         Uuid::now_v7(),
    //         "test item".into(),
    //         FractionalIndex::new_after(&index),
    //     ).unwrap();
    //     crdt.add_item(
    //         Uuid::now_v7(),
    //         "test 4 item".into(),
    //         FractionalIndex::new_after(&index),
    //     ).unwrap();
    //     crdt.add_item(
    //         Uuid::now_v7(),
    //         "test 3 asdf item".into(),
    //         FractionalIndex::new_before(&index),
    //     ).unwrap();
    // }

    // #[test]
    // fn join_in_memory_stored_replicas() {
    //     let mut crdt_1 = ChecklistCrdt::new(
    //         InMemoryStorage::new(),
    //     ).unwrap();

    //     let head_id = crdt_1.add_head(None, "A".into(), Some("ab c de".into())).unwrap();
    //     let position = FractionalIndex::default();
    //     let first = crdt_1.add_item(head_id.clone(), "1.".into(), position.clone()).unwrap();
    //     let second = crdt_1.add_item(head_id.clone(), "2.".into(), FractionalIndex::new_after(&position)).unwrap();

    //     let replica_data = crdt_1.fork().unwrap();
    //     let mut crdt_2 = ChecklistCrdt::new(
    //         InMemoryStorage::init(
    //             replica_data.stamp,
    //             replica_data.delta.operations.head_operations,
    //             replica_data.delta.operations.item_operations
    //         ),
    //     ).unwrap();

    //     crdt_1.update_head_name(&head_id, "B".into()).unwrap();
    //     crdt_2.update_head_name(&head_id, "C".into()).unwrap();

    //     crdt_1.update_item_checked(&first, true).unwrap();
    //     crdt_2.update_item_checked(&second, true).unwrap();
    //     crdt_2.update_item_name(&second, "3.2".into()).unwrap();
    //     crdt_1.update_item_name(&second, "3.1".into()).unwrap();

    //     let delta_1_2 = crdt_1.get_delta_since(crdt_2.itc_stamp.history()).unwrap();
    //     let delta_2_1 = crdt_2.get_delta_since(crdt_1.itc_stamp.history()).unwrap();
    //     assert_ne!(delta_1_2, delta_2_1);

    //     crdt_1.apply_delta(delta_2_1).unwrap();
    //     crdt_2.apply_delta(delta_1_2).unwrap();

    //     let replica_1 = crdt_1.fork().unwrap();
    //     let replica_2 = crdt_2.fork().unwrap();
    //     assert_eq!(replica_1.delta.operations, replica_2.delta.operations);
    // }

    // #[test]
    // fn join_in_file_stored_replicas() {
    //     let storage_1 = FileStorage::new("stamp_1.txt", "head_1.txt", "item_1.txt").unwrap();
    //     let storage_2 = FileStorage::new("stamp_2.txt", "head_2.txt", "item_2.txt").unwrap();
    //     let mut crdt_1 = ChecklistCrdt::new(storage_1).unwrap();

    //     let head_id = crdt_1.add_head(None, "A".into(), Some("ab c de".into())).unwrap();
    //     let position = FractionalIndex::default();
    //     let first = crdt_1.add_item(head_id.clone(), "1.".into(), position.clone()).unwrap();
    //     let second = crdt_1.add_item(head_id.clone(), "2.".into(), FractionalIndex::new_after(&position)).unwrap();

    //     let replica_data = crdt_1.fork().unwrap();
    //     let mut crdt_2 = ChecklistCrdt::new_from(storage_2, replica_data).unwrap();

    //     crdt_1.update_head_name(&head_id, "B".into()).unwrap();
    //     crdt_2.update_head_name(&head_id, "C".into()).unwrap();

    //     crdt_1.update_item_checked(&first, true).unwrap();
    //     crdt_2.update_item_checked(&second, true).unwrap();
    //     crdt_2.update_item_name(&second, "3.2".into()).unwrap();
    //     crdt_1.update_item_name(&second, "3.1".into()).unwrap();

    //     let delta_1_2 = crdt_1.get_delta_since(crdt_2.itc_stamp.history()).unwrap();
    //     let delta_2_1 = crdt_2.get_delta_since(crdt_1.itc_stamp.history()).unwrap();
    //     assert_ne!(delta_1_2, delta_2_1);

    //     crdt_1.apply_delta(delta_2_1).unwrap();
    //     crdt_2.apply_delta(delta_1_2).unwrap();

    //     let replica_1 = crdt_1.fork().unwrap();
    //     let replica_2 = crdt_2.fork().unwrap();
    //     assert_eq!(replica_1.delta.operations, replica_2.delta.operations);
    // }
}
