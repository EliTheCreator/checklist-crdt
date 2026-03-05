use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{head, item};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStorage;

impl Store<'_> for ErrorStorage {
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

    fn save_head_operation(&mut self, _: head::Operation) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_head_tombstone(&mut self, _: head::Tombstone) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_head_operations(&mut self) -> Result<Vec<head::Operation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_head_tombstones(&mut self) -> Result<Vec<head::Tombstone>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_associated_head_operations(&mut self, _: &Uuid) -> Result<Vec<head::Operation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_associated_head_tombstone(&mut self, _: &Uuid) -> Result<Option<head::Tombstone>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_head_operation(&mut self, _: &Uuid) -> Result<head::Operation, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_head_tombstone(&mut self, _: &Uuid) -> Result<head::Tombstone, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_item_operation(&mut self, _: item::Operation) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn save_item_tombstone(&mut self, _: item::Tombstone) -> Result<(), StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_item_operations(&mut self) -> Result<Vec<item::Operation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_item_tombstones(&mut self) -> Result<Vec<item::Tombstone>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_associated_item_operations(&mut self, _: &Uuid) -> Result<Vec<item::Operation>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn load_associated_item_tombstone(&mut self, _: &Uuid) -> Result<Option<item::Tombstone>, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_item_operation(&mut self, _: &Uuid) -> Result<item::Operation, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }

    fn erase_item_tombstone(&mut self, _: &Uuid) -> Result<item::Tombstone, StorageError> {
        bail!(StorageError::backend_specific("this storage always returns an error"))
    }
}
