use core::future::Future;
use core::pin::Pin;

use exn::Result;
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;


pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;


pub trait Store {
    type EmtpyFuture<'a>: Future<Output = Result<(), StorageError>> + 'a
    where
        Self: 'a;

    type BoolFuture<'a>: Future<Output = Result<bool, StorageError>> + 'a
    where
        Self: 'a;

    type StampFuture<'a>: Future<Output = Result<Stamp, StorageError>> + 'a
    where
        Self: 'a;

    type HeadFuture<'a>: Future<Output = Result<HeadOperation, StorageError>> + 'a
    where
        Self: 'a;

    type VecHeadFuture<'a>: Future<Output = Result<Vec<HeadOperation>, StorageError>> + 'a
    where
        Self: 'a;

    type ItemFuture<'a>: Future<Output = Result<ItemOperation, StorageError>> + 'a
    where
        Self: 'a;

    type VecItemFuture<'a>: Future<Output = Result<Vec<ItemOperation>, StorageError>> + 'a
    where
        Self: 'a;

    fn start_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a>;
    fn abort_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a>;
    fn commit_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a>;

    fn save_stamp<'a>(&'a mut self, stamp: Stamp) -> Self::EmtpyFuture<'a>;
    fn load_stamp<'a>(&'a mut self) -> Self::StampFuture<'a>;

    fn save_head_operation<'a>(&'a mut self, operation: HeadOperation) -> Self::EmtpyFuture<'a>;
    fn load_all_head_operations<'a>(&'a mut self) -> Self::VecHeadFuture<'a>;
    fn load_all_associated_head_operations<'a>(&'a mut self, head_id: Uuid) -> Self::VecHeadFuture<'a>;
    fn erase_head_operation<'a>(&'a mut self, id: Uuid) -> Self::HeadFuture<'a>;

    fn save_item_operation<'a>(&'a mut self, operation: ItemOperation) -> Self::EmtpyFuture<'a>;
    fn load_all_item_operations<'a>(&'a mut self) -> Self::VecItemFuture<'a>;
    fn load_all_associated_item_operations<'a>(&'a mut self, item_id: Uuid) -> Self::VecItemFuture<'a>;
    fn erase_item_operation<'a>(&'a mut self, id: Uuid) -> Self::ItemFuture<'a>;
}
