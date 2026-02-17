use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use super::error::storage_error::StorageError;
use super::model::checklist::head::HeadEvent;
use super::model::checklist::item::ItemEvent;


pub trait Store {
    fn start_transaction(&mut self) -> Result<bool, StorageError>;
    fn abort_transaction(&mut self) -> Result<bool, StorageError>;
    fn commit_transaction(&mut self) -> Result<bool, StorageError>;

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    fn save_head_event(&mut self, event: &HeadEvent) -> Result<(), StorageError>;
    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError>;
    fn delete_head_event(&mut self, id: &Uuid) -> Result<HeadEvent, StorageError>;

    fn save_item_event(&mut self, event: &ItemEvent) -> Result<(), StorageError>;
    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError>;
    fn delete_item_event(&mut self, id: &Uuid) -> Result<ItemEvent, StorageError>;
}
