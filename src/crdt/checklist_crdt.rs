use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::discriminant;

use exn::{bail, Exn, Result, ResultExt};
use itc::{EventTree, IdTree, IntervalTreeClock, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::crdt::crdt_error::CrdtError;
use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::{StorageError, ErrorKind};
use crate::persistence::storage::Store;


#[macro_export]
macro_rules! transaction {
    ($this:expr, $stamp:expr, $body:block) => {{
        $this.storage
            .start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        let result = (|| -> Result<_, _> { $body })()?;

        let _ = $this.storage.save_stamp(&$stamp).map_err(|e| {
            $this.abort_transaction(e, "crdt unable to save stamp")
        })?;

        let _ = $this.storage.commit_transaction().map_err(|e| {
            $this.abort_transaction(e, "crdt unable commit transaction")
        })?;

        $this.itc_stamp = $stamp;

        Ok(result)
    }};
}


pub struct OperationDelta {
    from_itc_event: EventTree,
    to_itc_event: EventTree,
    head_operations: Vec<HeadOperation>,
    item_operations: Vec<ItemOperation>,
}

impl OperationDelta {
    pub fn new(
        from_itc_event: EventTree,
        to_itc_event: EventTree,
        head_operations: Vec<HeadOperation>,
        item_operations: Vec<ItemOperation>,
    ) -> Self {
        Self { from_itc_event, to_itc_event, head_operations, item_operations }
    }
}


#[derive(Debug, PartialEq)]
pub struct Peer {
    stamp: Stamp,
    head_operations: Vec<HeadOperation>,
    item_operations: Vec<ItemOperation>,
}

impl Peer {
    pub fn new(
        stamp: Stamp,
        head_operations: Vec<HeadOperation>,
        item_operations: Vec<ItemOperation>,
    ) -> Self {
        Self { stamp, head_operations, item_operations }
    }
}


pub struct ChecklistCrdt<S: Store> {
    storage: S,
    itc_stamp: Stamp
}

impl<S: Store> ChecklistCrdt<S> {
    pub fn new(mut storage: S) -> Result<Self, CrdtError> {
        let stamp = match storage.load_stamp() {
            Ok(s) => s,
            Err(e) if e.kind == ErrorKind::StampNone =>{
                let stamp = Stamp::seed();
                storage.save_stamp(&stamp)
                    .or_raise(|| CrdtError::fatal("failed to save new stamp"))?;
                stamp
            },
            Err(_) => bail!(CrdtError::fatal("failed to load stamp")),
        };

        Ok(ChecklistCrdt {
            storage: storage,
            itc_stamp: stamp,
        })
    }

    fn abort_transaction(
        &mut self,
        e1: Exn<StorageError>,
        text: &str,
    ) -> Exn<CrdtError> {
        let success_text = format!("{text}. all operations have been reverted");
        let failure_text = format!("{text}. unable to reverse all operations. crdt is in inconsistent state");
        match self.storage.abort_transaction() {
            Ok(_) => {
                e1.raise(CrdtError::recovered(success_text))
            },
            Err(e2) => {
                Exn::raise_all(CrdtError::fatal(failure_text), vec![e1, e2])
            },
        }
    }

    fn save_checklist_head_operation(&mut self, operation: HeadOperation) -> Result<(), CrdtError> {
        self.storage.save_head_operation(&operation).map_err(|e|
            self.abort_transaction(e, "crdt unable to store head operation")
        )
    }

    fn erase_checklist_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, CrdtError> {
        self.storage.erase_head_operation(&id).map_err(|e|
            self.abort_transaction(e, "crdt unable to erase head operation")
        )
    }

    pub fn add_checklist_head(
        &mut self,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let operation = HeadOperation::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            template_id: template_id,
            name: name,
            description: description,
        };

        let _: Result<(), CrdtError> = transaction!(self, stamp, {
            self.save_checklist_head_operation(operation)
        });
        Ok(id)
    }

    pub fn update_checklist_head_name(
        &mut self,
        head_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::NameUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            name: name,
        };

        transaction!(self, stamp, { self.save_checklist_head_operation(operation) })
    }

    pub fn update_checklist_head_description(
        &mut self,
        head_id: &Uuid,
        description: Option<String>,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::DescriptionUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            description: description,
        };

        transaction!(self, stamp, { self.save_checklist_head_operation(operation) })
    }

    pub fn update_checklist_head_completed(
        &mut self,
        head_id: &Uuid,
        completed: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::CompletedUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            completed: completed,
        };

        transaction!(self, stamp, { self.save_checklist_head_operation(operation) })
    }

    pub fn delete_checklist_head(
        &mut self,
        head_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = HeadOperation::Deletion {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            head_id: head_id.clone(),
        };

        transaction!(self, stamp, { self.save_checklist_head_operation(operation) })
    }

    pub fn get_all_checklist_heads(&self) -> Result<Vec<HeadOperation>, CrdtError> {
        self.storage.load_all_head_operations()
            .or_raise(|| CrdtError::recovered("crdt unable to load all head operations"))
    }

    fn save_checklist_item_operation(&mut self, operation: ItemOperation) -> Result<(), CrdtError> {
        self.storage.save_item_operation(&operation).map_err(|e|
            self.abort_transaction(e, "crdt unable to store item operation")
        )
    }

    fn erase_checklist_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, CrdtError> {
        self.storage.erase_item_operation(id).map_err(|e|
            self.abort_transaction(e, "crdt unable to erase item operation")
        )
    }

    pub fn add_checklist_item(
        &mut self,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let operation = ItemOperation::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            head_id: head_id,
            name: name,
            position: position,
        };

        let _: Result<(), CrdtError> = transaction!(self, stamp, {
            self.save_checklist_item_operation(operation)
        });
        Ok(id)
    }

    pub fn update_checklist_item_name(
        &mut self,
        item_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::NameUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            name: name,
        };

        transaction!(self, stamp, { self.save_checklist_item_operation(operation) })
    }

    pub fn update_checklist_item_position(
        &mut self,
        item_id: &Uuid,
        position: FractionalIndex,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::PositionUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            position: position,
        };

        transaction!(self, stamp, { self.save_checklist_item_operation(operation) })
    }

    pub fn update_checklist_item_checked(
        &mut self,
        item_id: &Uuid,
        checked: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation = ItemOperation::CheckedUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            checked: checked,
        };

        transaction!(self, stamp, { self.save_checklist_item_operation(operation) })
    }

    pub fn delete_checklist_item(
        &mut self,
        item_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let operation: ItemOperation = ItemOperation::Deletion {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
        };

        transaction!(self, stamp, { self.save_checklist_item_operation(operation) })
    }

    pub fn get_all_checklist_items(&self) -> Result<Vec<ItemOperation>, CrdtError> {
        self.storage.load_all_item_operations()
            .or_raise(|| CrdtError::recovered("crdt unable to load all item operations"))
    }

    pub fn get_operation_delta(&self, peer_operation: EventTree) -> Result<OperationDelta, CrdtError> {
        let mut head_operations = self.get_all_checklist_heads()?;
        head_operations.retain(|head| {
            match peer_operation.partial_cmp(head.itc_event()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let mut item_operations = self.get_all_checklist_items()?;
        item_operations.retain(|item| {
            match peer_operation.partial_cmp(item.itc_event()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let own_operation = self.itc_stamp.event_tree();

        Ok(OperationDelta::new(peer_operation, own_operation, head_operations, item_operations))
    }

    pub fn apply_operation_delta(&mut self, delta: OperationDelta) -> Result<(), CrdtError> {
        // if self.itc_stamp.event_tree() != delta.from_itc_event {
        //     bail!(CrdtError::recovered(""));
        // }

        let peer_stamp = Stamp::new(IdTree::zero(), delta.to_itc_event.clone());
        let stamp = self.itc_stamp.join(&peer_stamp).event();

        transaction!(self, stamp, {
            for operation in delta.head_operations {
                self.save_checklist_head_operation(operation)?;
            }

            for operation in delta.item_operations {
                self.save_checklist_item_operation(operation)?;
            }

            Ok(())
        })
    }

    pub fn fork(&mut self) -> Result<Peer, CrdtError> {
        let (stamp, peer_stamp) = self.itc_stamp.fork();
        let head_operations = self.get_all_checklist_heads()?;
        let item_operations = self.get_all_checklist_items()?;

        let _: Result<(), CrdtError> = transaction!(self, stamp, { Ok(()) });
        Ok(Peer::new(peer_stamp, head_operations, item_operations))
    }

    fn find_trimable_head_operations(&self) -> Result<(Vec<Uuid>, HashSet<Uuid>), CrdtError> {
        let operations = self.get_all_checklist_heads()?;
        let grouped_operations = operations.iter()
            .fold(HashMap::new(), |mut m: HashMap<Uuid, Vec<&HeadOperation>>, e| {
                m.entry(e.head_id().clone()).or_default().push(e);
                m
            });

        let mut trimable_operations = Vec::new();
        let mut deleted_operations = Vec::new();
        let deletion_discriminant = discriminant(&HeadOperation::Deletion { id: Uuid::default(), head_id: Uuid::default(), itc_event: EventTree::zero() });
        for group in grouped_operations.into_values() {
            let discriminant_bins = group.into_iter()
                .fold(HashMap::new(), |mut m: HashMap<_,Vec<&HeadOperation>>, e| {
                    m.entry(discriminant(e)).or_default().push(e);
                    m
                });

            let mut keep = Vec::new();
            let mut headstone = None;
            for mut bin in discriminant_bins.into_values() {
                if let Some(operation) = bin.pop() {
                    if discriminant(operation) == deletion_discriminant {
                        headstone = Some(operation)
                    } else {
                        keep.push(operation);
                    }
                    trimable_operations.extend( bin);
                }
            }
            if let Some(headstone) = headstone {
                trimable_operations.extend(keep);
                deleted_operations.push(headstone);
            }
        }

        let trimable_operations_ids = trimable_operations.into_iter()
            .map(|operation| operation.id().clone())
            .collect::<Vec<Uuid>>();

        let deleted_operations_ids = deleted_operations.into_iter()
            .map(|operation| {
                match operation {
                    HeadOperation::Deletion { head_id, .. } => head_id.clone(),
                    _ => unreachable!()
                }
            })
            .collect::<HashSet<Uuid>>();

        Ok((trimable_operations_ids, deleted_operations_ids))
    }

    fn find_trimable_item_operations(
        &self,
        deleted_head_operations_ids: HashSet<Uuid>
    ) -> Result<Vec<Uuid>, CrdtError> {
        let operations = self.get_all_checklist_items()?;
        let grouped_operations = operations.iter()
            .fold(HashMap::new(), |mut m: HashMap<Uuid, Vec<&ItemOperation>>, e| {
                m.entry(e.item_id().clone()).or_default().push(e);
                m
            });

        let mut trimable_operations = Vec::new();
        let mut deleted_operations = Vec::new();
        for group in grouped_operations.into_values() {
            let discriminant_bins = group.into_iter()
                .fold(HashMap::new(), |mut m: HashMap<_,Vec<&ItemOperation>>, e| {
                    m.entry(discriminant(e)).or_default().push(e);
                    m
                });

            let mut keep = Vec::new();
            let mut headstone = None;
            let mut delete_all = false;
            for mut bin in discriminant_bins.into_values() {
                let operation = match bin.pop() {
                    Some(e) => e,
                    None => continue,
                };
                match operation {
                    ItemOperation::Creation { head_id, .. } => {
                        if deleted_head_operations_ids.contains(&head_id) {
                            delete_all = true;
                        }
                        keep.push(operation);
                    },
                    ItemOperation::Deletion { .. } => headstone = Some(operation),
                    _ => keep.push(operation),
                }
                trimable_operations.extend( bin);
            }

            if delete_all {
                trimable_operations.extend(keep);
                if let Some(headstone) = headstone {
                    trimable_operations.push(headstone);
                }
            } else if let Some(headstone) = headstone {
                trimable_operations.extend(keep);
                deleted_operations.push(headstone);
            }
        }

        let trimable_operations_ids = trimable_operations.into_iter()
            .map(|operation| operation.id().clone())
            .collect::<Vec<Uuid>>();

        Ok(trimable_operations_ids)
    }

    pub fn trim_operations(&mut self) -> Result<(Vec<HeadOperation>, Vec<ItemOperation>), CrdtError> {
        let (trimable_head_operations_ids, deleted_head_operations_ids) = self
            .find_trimable_head_operations()
            .or_raise(|| CrdtError::recoverable("failed to find all trimable head operations"))?;
        let trimable_item_operations_ids = self.find_trimable_item_operations(deleted_head_operations_ids)
            .or_raise(|| CrdtError::recoverable("failed to find all trimable item operations"))?;

        let stamp = self.itc_stamp.event();

        transaction!(self, stamp, {
            let deleted_item_operations = trimable_item_operations_ids.into_iter()
                .map(|id| self.erase_checklist_item_operation(&id))
                .collect::<Result<Vec<ItemOperation>, CrdtError>>()?;
            let deleted_head_operations = trimable_head_operations_ids.into_iter()
                .map(|id| self.erase_checklist_head_operation(&id))
                .collect::<Result<Vec<HeadOperation>, CrdtError>>()?;

            Ok((deleted_head_operations, deleted_item_operations))
        })
    }
}


#[cfg(test)]
mod test {
    use crate::persistence::storage::InMemoryStorage;

    use super::*;

    #[test]
    fn init_test() {
        use crate::persistence::storage::FileStorage;

        let stamp_path = "./stamp.txt";
        let head_log_path = "./head_log.txt";
        let item_log_path = "./item_log.txt";
        let file_store = FileStorage::new(
            stamp_path,
            head_log_path,
            item_log_path,
        ).unwrap();


        let mut crdt = ChecklistCrdt::new(file_store).unwrap();
        let index = FractionalIndex::default();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test item".into(),
            FractionalIndex::new_after(&index),
        ).unwrap();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test 4 item".into(),
            FractionalIndex::new_after(&index),
        ).unwrap();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test 3 asdf item".into(),
            FractionalIndex::new_before(&index),
        ).unwrap();
    }

    #[test]
    fn merge_test() {
        let mut crdt_1 = ChecklistCrdt::new(
            InMemoryStorage::new(),
        ).unwrap();

        let head_id = crdt_1.add_checklist_head(None, "A".into(), Some("ab c de".into())).unwrap();
        let position = FractionalIndex::default();
        let first = crdt_1.add_checklist_item(head_id.clone(), "1.".into(), position.clone()).unwrap();
        let second = crdt_1.add_checklist_item(head_id.clone(), "2.".into(), FractionalIndex::new_after(&position)).unwrap();

        let peer_data = crdt_1.fork().unwrap();
        let mut crdt_2 = ChecklistCrdt::new(
            InMemoryStorage::init(peer_data.stamp, peer_data.head_operations, peer_data.item_operations),
        ).unwrap();

        crdt_1.update_checklist_head_name(&head_id, "B".into()).unwrap();
        crdt_2.update_checklist_head_name(&head_id, "C".into()).unwrap();

        crdt_1.update_checklist_item_checked(&first, true).unwrap();
        crdt_2.update_checklist_item_checked(&second, true).unwrap();
        crdt_2.update_checklist_item_name(&second, "3.2".into()).unwrap();
        crdt_1.update_checklist_item_name(&second, "3.1".into()).unwrap();

        let delta_1_2 = crdt_1.get_operation_delta(crdt_2.itc_stamp.event_tree()).unwrap();
        let delta_2_1 = crdt_2.get_operation_delta(crdt_1.itc_stamp.event_tree()).unwrap();

        crdt_1.apply_operation_delta(delta_2_1).unwrap();
        crdt_2.apply_operation_delta(delta_1_2).unwrap();

        let mut peer_1 = crdt_1.fork().unwrap();
        peer_1.stamp = Stamp::seed();
        let mut peer_2 = crdt_2.fork().unwrap();
        peer_2.stamp = Stamp::seed();

        assert_eq!(peer_1, peer_2);
    }
}
