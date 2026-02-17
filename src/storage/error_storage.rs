use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use super::model::checklist::head::HeadEvent;
use super::model::checklist::item::ItemEvent;
use super::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStorage;

impl Store for ErrorStorage {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_stamp(&mut self, _: &Stamp) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_head_event(&mut self, _: &HeadEvent) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn delete_head_event(&mut self, _: &Uuid) -> Result<HeadEvent, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_item_event(&mut self, _: &ItemEvent) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn delete_item_event(&mut self, _: &Uuid) -> Result<ItemEvent, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }
}
