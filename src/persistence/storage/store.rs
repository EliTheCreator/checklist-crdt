use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;


pub trait Store {
    fn start_transaction(&mut self) -> Result<bool, StorageError>;
    fn abort_transaction(&mut self) -> Result<bool, StorageError>;
    fn commit_transaction(&mut self) -> Result<bool, StorageError>;

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError>;
    fn load_all_head_operations(&self) -> Result<Vec<HeadOperation>, StorageError>;
    fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError>;

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError>;
    fn load_all_item_operations(&self) -> Result<Vec<ItemOperation>, StorageError>;
    fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError>;
}
