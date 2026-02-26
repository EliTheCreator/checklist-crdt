use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;


pub trait Store {
    async fn start_transaction(&mut self) -> Result<bool, StorageError>;
    async fn abort_transaction(&mut self) -> Result<bool, StorageError>;
    async fn commit_transaction(&mut self) -> Result<bool, StorageError>;

    async fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    async fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    async fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError>;
    async fn load_all_head_operations(&mut self) -> Result<Vec<HeadOperation>, StorageError>;
    async fn load_all_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<HeadOperation>, StorageError>;
    async fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError>;

    async fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError>;
    async fn load_all_item_operations(&mut self) -> Result<Vec<ItemOperation>, StorageError>;
    async fn load_all_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<ItemOperation>, StorageError>;
    async fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError>;
}
