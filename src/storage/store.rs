use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use super::storage_error::StorageError;
use super::model::{ChecklistHeadEvent, ChecklistItemEvent};


pub trait Store {
    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    fn save_head(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError>;
    fn load_all_heads(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError>;
    fn delete_head(&mut self, head_id: &Uuid) -> Result<bool, StorageError>;

    fn save_item(&mut self, event: &ChecklistItemEvent) -> Result<(), StorageError>;
    fn load_all_items(&self) -> Result<Vec<ChecklistItemEvent>, StorageError>;
    fn delete_item(&mut self, item_id: &Uuid) -> Result<bool, StorageError>;
}
