use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use super::model::{ChecklistHeadEvent, ChecklistItemEvent};
use super::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStore;

impl Store for ErrorStore {
    fn save_stamp(&mut self, _: &Stamp) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }
    
    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn save_head(&mut self, _: &ChecklistHeadEvent) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_heads(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn delete_head(&mut self, _: &Uuid) -> Result<bool, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn save_item(&mut self, _: &ChecklistItemEvent) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_items(&self) -> Result<Vec<ChecklistItemEvent>, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn delete_item(&mut self, _: &Uuid) -> Result<bool, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }
}
