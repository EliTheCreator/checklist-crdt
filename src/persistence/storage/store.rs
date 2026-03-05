use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{head, item};
use crate::persistence::storage_error::StorageError;


pub trait Store<'a> {
    fn start_transaction(&'a mut self) -> Result<bool, StorageError>;
    fn abort_transaction(&mut self) -> Result<bool, StorageError>;
    fn commit_transaction(&mut self) -> Result<bool, StorageError>;

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError>;
    fn load_stamp(&mut self) -> Result<Stamp, StorageError>;

    fn save_head_operation(&mut self, operation: head::Operation) -> Result<(), StorageError>;
    fn save_head_tombstone(&mut self, tombstone: head::Tombstone) -> Result<(), StorageError>;
    fn load_head_operations(&mut self) -> Result<Vec<head::Operation>, StorageError>;
    fn load_head_tombstones(&mut self) -> Result<Vec<head::Tombstone>, StorageError>;
    fn load_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<head::Operation>, StorageError>;
    fn load_associated_head_tombstone(&mut self, head_id: &Uuid) -> Result<Option<head::Tombstone>, StorageError>;
    fn erase_head_operation(&mut self, id: &Uuid) -> Result<head::Operation, StorageError>;
    fn erase_head_tombstone(&mut self, id: &Uuid) -> Result<head::Tombstone, StorageError>;

    fn save_item_operation(&mut self, operation: item::Operation) -> Result<(), StorageError>;
    fn save_item_tombstone(&mut self, tombstone: item::Tombstone) -> Result<(), StorageError>;
    fn load_item_operations(&mut self) -> Result<Vec<item::Operation>, StorageError>;
    fn load_item_tombstones(&mut self) -> Result<Vec<item::Tombstone>, StorageError>;
    fn load_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<item::Operation>, StorageError>;
    fn load_associated_item_tombstone(&mut self, item_id: &Uuid) -> Result<Option<item::Tombstone>, StorageError>;
    fn erase_item_operation(&mut self, id: &Uuid) -> Result<item::Operation, StorageError>;
    fn erase_item_tombstone(&mut self, id: &Uuid) -> Result<item::Tombstone, StorageError>;
}
