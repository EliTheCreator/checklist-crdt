use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use super::storage_error::StorageError;
use super::model::checklist::head::ChecklistHeadEvent;
use super::model::checklist::item::ChecklistItemEvent;


pub trait Store {
    fn start_transaction(&mut self) -> Result<bool, StorageError>;
    fn abort_transaction(&mut self) -> Result<bool, StorageError>;
    fn commit_transaction(&mut self) -> Result<bool, StorageError>;

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    fn save_head_event(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError>;
    fn load_all_head_events(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError>;
    fn delete_head_event(&mut self, head_id: &Uuid) -> Result<bool, StorageError>;

    fn save_item_event(&mut self, event: &ChecklistItemEvent) -> Result<(), StorageError>;
    fn load_all_item_events(&self) -> Result<Vec<ChecklistItemEvent>, StorageError>;
    fn delete_item_event(&mut self, item_id: &Uuid) -> Result<bool, StorageError>;
}
