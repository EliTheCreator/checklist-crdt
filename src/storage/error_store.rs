use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use super::model::checklist::head::ChecklistHeadEvent;
use super::model::checklist::item::ChecklistItemEvent;
use super::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStore;

impl Store for ErrorStore {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        todo!()
    }

    fn save_stamp(&mut self, _: &Stamp) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn save_head_event(&mut self, _: &ChecklistHeadEvent) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_head_events(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn delete_head_event(&mut self, _: &Uuid) -> Result<bool, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn save_item_event(&mut self, _: &ChecklistItemEvent) -> Result<(), StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_item_events(&self) -> Result<Vec<ChecklistItemEvent>, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }

    fn delete_item_event(&mut self, _: &Uuid) -> Result<bool, StorageError> {
        Result::Err(StorageError("this store always returns an error".into()).into())
    }
}
