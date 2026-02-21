use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadEvent, ItemEvent};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


pub struct InMemoryStorage {
    head_events: Vec<HeadEvent>,
    item_events: Vec<ItemEvent>
}

impl Store for InMemoryStorage {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        todo!()
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        todo!()
    }

    fn save_head_event(&mut self, event: &HeadEvent) -> Result<(), StorageError> {
        todo!()
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        todo!()
    }

    fn delete_head_event(&mut self, id: &Uuid) -> Result<HeadEvent, StorageError> {
        todo!()
    }

    fn save_item_event(&mut self, event: &ItemEvent) -> Result<(), StorageError> {
        todo!()
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        todo!()
    }

    fn delete_item_event(&mut self, id: &Uuid) -> Result<ItemEvent, StorageError> {
        todo!()
    }
}
