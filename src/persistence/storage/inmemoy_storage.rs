use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadEvent, ItemEvent};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnOnce(&mut InMemoryStorage) -> Result<(), StorageError>>;


pub struct InMemoryStorage {
    stamp: Stamp,
    head_events: Vec<HeadEvent>,
    item_events: Vec<ItemEvent>,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            stamp: Stamp::seed(),
            head_events: Vec::new(),
            item_events: Vec::new(),
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

    fn save_head_event(&mut self, event: &HeadEvent) -> Result<(), StorageError> {
        let index = self.head_events.binary_search_by_key(event.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.head_events.insert(index, event.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.head_events.remove(index);
                Ok(())
            }));
        }

        Ok(())
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        Ok(self.head_events.clone())
    }

    fn delete_head_event(&mut self, id: &Uuid) -> Result<HeadEvent, StorageError> {
        let index = match self.head_events.binary_search_by_key(id, |h| *h.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a head event with id '{id}'")
            )),
        };

        let head_event = self.head_events.remove(index);
        Ok(head_event)
    }

    fn save_item_event(&mut self, event: &ItemEvent) -> Result<(), StorageError> {
        let index = self.item_events.binary_search_by_key(event.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.item_events.insert(index, event.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.item_events.remove(index);
                Ok(())
            }));
        }

        Ok(())
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        Ok(self.item_events.clone())
    }

    fn delete_item_event(&mut self, id: &Uuid) -> Result<ItemEvent, StorageError> {
        let index = match self.item_events.binary_search_by_key(id, |i| *i.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a item event with id '{id}'")
            )),
        };

        let item_event = self.item_events.remove(index);
        Ok(item_event)
    }
}
