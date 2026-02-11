use exn::Result;

use super::storage_error::StorageError;
use super::model::{ChecklistHeadEvent, ChecklistItemEvent};


// pub trait Store {
//     fn save_head(&self, event: &CheckListHeadEvent) -> Result<(), StorageError>;
//     fn save_item(&self, event: &CheckListItemEvent) -> Result<(), StorageError>;
//     fn load_head(&self) -> Result<CheckListHeadEvent, StorageError>;
//     fn load_item(&self) -> Result<CheckListItemEvent, StorageError>;
// }

pub trait Store: StoreHead + StoreItem {}

pub trait StoreHead {
    fn save(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError>;
    fn load(&self) -> Result<ChecklistHeadEvent, StorageError>;
}

pub trait StoreItem {
    fn save(&mut self, event: &ChecklistItemEvent) -> Result<(), StorageError>;
    fn load(&self) -> Result<ChecklistItemEvent, StorageError>;
}
