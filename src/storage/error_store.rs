use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use super::model::checklist::head::HeadEvent;
use super::model::checklist::item::ItemEvent;
use super::error::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStore;

impl Store for ErrorStore {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn save_stamp(&mut self, _: &Stamp) -> Result<(), StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn save_head_event(&mut self, _: &HeadEvent) -> Result<(), StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn delete_head_event(&mut self, _: &Uuid) -> Result<HeadEvent, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn save_item_event(&mut self, _: &ItemEvent) -> Result<(), StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }

    fn delete_item_event(&mut self, _: &Uuid) -> Result<ItemEvent, StorageError> {
        bail!(StorageError("this store always returns an error".into()).into())
    }
}
