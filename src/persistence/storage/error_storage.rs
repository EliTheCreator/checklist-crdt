use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;
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

    fn save_head_operation(&mut self, _: HeadOperation) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_head_operations(&self) -> Result<Vec<HeadOperation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_associated_head_operations(&mut self, _: &Uuid) -> Result<Vec<HeadOperation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_head_operation(&mut self, _: &Uuid) -> Result<HeadOperation, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_item_operation(&mut self, _: ItemOperation) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_item_operations(&self) -> Result<Vec<ItemOperation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_all_associated_item_operations(&mut self, _: &Uuid) -> Result<Vec<ItemOperation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_item_operation(&mut self, _: &Uuid) -> Result<ItemOperation, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }
}
