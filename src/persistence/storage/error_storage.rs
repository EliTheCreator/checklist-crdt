use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage::BoxedFuture;
use crate::persistence::storage_error::StorageError;
use super::store::Store;


pub struct ErrorStorage;

impl Store for ErrorStorage {
    type EmtpyFuture<'a> = BoxedFuture<'a, Result<(), StorageError>> where Self: 'a;
    type BoolFuture<'a> = BoxedFuture<'a, Result<bool, StorageError>> where Self: 'a;
    type StampFuture<'a> = BoxedFuture<'a, Result<Stamp, StorageError>> where Self: 'a;
    type HeadFuture<'a> = BoxedFuture<'a, Result<HeadOperation, StorageError>> where Self: 'a;
    type VecHeadFuture<'a> = BoxedFuture<'a, Result<Vec<HeadOperation>, StorageError>> where Self: 'a;
    type ItemFuture<'a> = BoxedFuture<'a, Result<ItemOperation, StorageError>> where Self: 'a;
    type VecItemFuture<'a> = BoxedFuture<'a, Result<Vec<ItemOperation>, StorageError>> where Self: 'a;


    fn start_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn abort_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn commit_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn save_stamp<'a>(&'a mut self, _: Stamp) -> Self::EmtpyFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn load_stamp<'a>(&'a mut self) -> Self::StampFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn save_head_operation<'a>(&'a mut self, _: HeadOperation) -> Self::EmtpyFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn load_all_head_operations<'a>(&'a mut self) -> Self::VecHeadFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn load_all_associated_head_operations<'a>(&'a mut self, _: Uuid) -> Self::VecHeadFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn erase_head_operation<'a>(&'a mut self, _: Uuid) -> Self::HeadFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn save_item_operation<'a>(&'a mut self, _: ItemOperation) -> Self::EmtpyFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn load_all_item_operations<'a>(&'a mut self) -> Self::VecItemFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn load_all_associated_item_operations<'a>(&'a mut self, _: Uuid) -> Self::VecItemFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }

    fn erase_item_operation<'a>(&'a mut self, _: Uuid) -> Self::ItemFuture<'a> {
        Box::pin(async move {
            bail!(StorageError::backend_specific("this storage always returns an error"))
        })
    }
}
