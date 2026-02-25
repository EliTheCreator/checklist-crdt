use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnOnce(&mut InMemoryStorage) -> Result<(), StorageError>>;


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

    pub fn init(
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

impl Store for InMemoryStorage {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        let return_value = !self.in_transaction;
        self.in_transaction = true;
        Ok(return_value)
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }

        while let Some(rollback_function) = self.rollback_stack.pop() {
            if let Err(e) = rollback_function(self) {
                self.rollback_stack.clear();
                return Result::Err(e.raise(
                    StorageError::transaction_rollback_partial("failed to rollback all transaction steps")
                ));
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
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.stamp = stamp_cpy;
                Ok(())
            }));
        }

        self.stamp = stamp.clone();
        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        Ok(self.stamp.clone())
    }

    fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError> {
        let index = self.head_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.head_operations.insert(index, operation.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.head_operations.remove(index);
                Ok(())
            }));
        }

        Ok(())
    }

    fn load_all_head_operations(&self) -> Result<Vec<HeadOperation>, StorageError> {
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
        Ok(head_operation)
    }

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError> {
        let index = self.item_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.item_operations.insert(index, operation.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.item_operations.remove(index);
                Ok(())
            }));
        }

        Ok(())
    }

    fn load_all_item_operations(&self) -> Result<Vec<ItemOperation>, StorageError> {
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
        Ok(item_operation)
    }
}
