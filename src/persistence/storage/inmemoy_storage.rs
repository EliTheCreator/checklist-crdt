use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


enum RollbackFunction {
    SaveHeadOperation(HeadOperation),
    EraseHeadOperation(Uuid),
    SaveItemOperation(ItemOperation),
    EraseItemOperation(Uuid),
    SaveStamp(Stamp),
}


pub struct InMemoryStorage {
    stamp: Stamp,
    head_operations: Vec<HeadOperation>,
    item_operations: Vec<ItemOperation>,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            stamp: Stamp::seed(),
            head_operations: Vec::new(),
            item_operations: Vec::new(),
            in_transaction: false,
            rollback_stack: Vec::new(),
        }
    }

    pub fn new_from(
        stamp: Stamp,
        head_operations: Vec<HeadOperation>,
        item_operations: Vec<ItemOperation>,
    ) -> Self {
        Self {
            stamp: stamp,
            head_operations,
            item_operations,
            in_transaction: false,
            rollback_stack: Vec::new(),
        }
    }
}

impl Store<'_> for InMemoryStorage {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        let return_value = !self.in_transaction;
        self.in_transaction = true;
        Ok(return_value)
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }

        let total_steps = self.rollback_stack.len();
        let mut current_step: usize = 0;
        while let Some(rollback_function) = self.rollback_stack.pop() {
            current_step += 1;

            use RollbackFunction::*;
            let rollback_result = match rollback_function {
                SaveHeadOperation(operation) => self.save_head_operation(operation),
                EraseHeadOperation(id) => self.erase_head_operation(&id).map(|_| ()),
                SaveItemOperation(operation) => self.save_item_operation(operation),
                EraseItemOperation(id) => self.erase_item_operation(&id).map(|_| ()),
                SaveStamp(stamp) => self.save_stamp(&stamp),
            };

            if let Err(e) = rollback_result {
                self.rollback_stack.clear();
                bail!(e.raise(StorageError::transaction_rollback_partial(format!(
                    "failed to rollback step {current_step} of {total_steps} of the transaction"
                ))));
            }
        }

        self.in_transaction = false;
        Ok(true)
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }
        self.rollback_stack.clear();
        Ok(true)
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        let stamp_cpy= self.stamp.clone();
        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::SaveStamp(stamp_cpy));
        }

        self.stamp = stamp.clone();
        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        Ok(self.stamp.clone())
    }

    fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError> {
        let id = operation.id().clone();

        let index = self.head_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.head_operations.insert(index, operation);

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::EraseHeadOperation(id));
        }

        Ok(())
    }

    fn load_all_head_operations(&mut self) -> Result<Vec<HeadOperation>, StorageError> {
        Ok(self.head_operations.clone())
    }

    fn load_all_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<HeadOperation>, StorageError> {
        let heads = self.head_operations.iter()
            .filter(|head| head.head_id() == head_id)
            .map(|head| head.clone())
            .collect();

        Ok(heads)
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError> {
        let index = match self.head_operations.binary_search_by_key(id, |h| *h.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a head operation with id '{id}'")
            )),
        };

        let head_operation = self.head_operations.remove(index);

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::SaveHeadOperation(head_operation.clone()));
        }

        Ok(head_operation)
    }

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError> {
        let id = operation.id().clone();

        let index = self.item_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.item_operations.insert(index, operation);

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::EraseItemOperation(id));
        }

        Ok(())
    }

    fn load_all_item_operations(&mut self) -> Result<Vec<ItemOperation>, StorageError> {
        Ok(self.item_operations.clone())
    }

    fn load_all_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<ItemOperation>, StorageError> {
        let items = self.item_operations.iter()
            .filter(|item| item.item_id() == item_id)
            .map(|item| item.clone())
            .collect();

        Ok(items)
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError> {
        let index = match self.item_operations.binary_search_by_key(id, |i| *i.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a item operation with id '{id}'")
            )),
        };

        let item_operation = self.item_operations.remove(index);

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::SaveItemOperation(item_operation.clone()));
        }

        Ok(item_operation)
    }
}
