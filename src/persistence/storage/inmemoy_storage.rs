use exn::{Result, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage::BoxedFuture;
use crate::persistence::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnOnce(&mut InMemoryStorage) -> Result<(), StorageError>>;


pub struct InMemoryStorage {
    stamp: Stamp,
    head_operations: Vec<HeadOperation>,
    item_operations: Vec<ItemOperation>,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl InMemoryStorage {
    pub async fn new() -> Self {
        Self {
            stamp: Stamp::seed(),
            head_operations: Vec::new(),
            item_operations: Vec::new(),
            in_transaction: false,
            rollback_stack: Vec::new(),
        }
    }

    pub async fn init(
        stamp: Stamp,
        head_operations: Vec<HeadOperation>,
        item_operations: Vec<ItemOperation>,
    ) -> Self {
        Self {
            stamp: stamp,
            head_operations,
            item_operations,
            in_transaction: false,
            rollback_stack: Vec::new(),
        }
    }
}

impl Store for InMemoryStorage {
    type EmtpyFuture<'a> = BoxedFuture<'a, Result<(), StorageError>> where Self: 'a;
    type BoolFuture<'a> = BoxedFuture<'a, Result<bool, StorageError>> where Self: 'a;
    type StampFuture<'a> = BoxedFuture<'a, Result<Stamp, StorageError>> where Self: 'a;
    type HeadFuture<'a> = BoxedFuture<'a, Result<HeadOperation, StorageError>> where Self: 'a;
    type VecHeadFuture<'a> = BoxedFuture<'a, Result<Vec<HeadOperation>, StorageError>> where Self: 'a;
    type ItemFuture<'a> = BoxedFuture<'a, Result<ItemOperation, StorageError>> where Self: 'a;
    type VecItemFuture<'a> = BoxedFuture<'a, Result<Vec<ItemOperation>, StorageError>> where Self: 'a;

    fn start_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
    Box::pin(async move {
        let return_value = !self.in_transaction;
        self.in_transaction = true;
        Ok(return_value)
    })}

    fn abort_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
    Box::pin(async move {
        if !self.in_transaction {
            return  Ok(false);
        }

        while let Some(rollback_function) = self.rollback_stack.pop() {
            if let Err(e) = rollback_function(self) {
                self.rollback_stack.clear();
                return Result::Err(e.raise(
                    StorageError::transaction_rollback_partial("failed to rollback all transaction steps")
                ));
            }
        }

        self.in_transaction = false;
        Ok(true)
    })}

    fn commit_transaction<'a>(&'a mut self) -> Self::BoolFuture<'a> {
    Box::pin(async move {
        if !self.in_transaction {
            return  Ok(false);
        }
        self.rollback_stack.clear();
        Ok(true)
    })}

    fn save_stamp<'a>(&'a mut self, stamp: Stamp) -> Self::EmtpyFuture<'a> {
    Box::pin(async move {
        let stamp_cpy= self.stamp.clone();
        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.stamp = stamp_cpy;
                Ok(())
            }));
        }

        self.stamp = stamp.clone();
        Ok(())
    })}

    fn load_stamp<'a>(&'a mut self) -> Self::StampFuture<'a> {
    Box::pin(async move {
        Ok(self.stamp.clone())
    })}

    fn save_head_operation<'a>(&'a mut self, operation: HeadOperation) -> Self::EmtpyFuture<'a> {
    Box::pin(async move {
        let index = self.head_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.head_operations.insert(index, operation.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.head_operations.remove(index);
                Ok(())
            }));
        }

        Ok(())
    })}

    fn load_all_head_operations<'a>(&'a mut self) -> Self::VecHeadFuture<'a> {
        Box::pin(async move {
            Ok(self.head_operations.clone())
        })
    }

    fn load_all_associated_head_operations<'a>(&'a mut self, head_id: Uuid) -> Self::VecHeadFuture<'a> {
    Box::pin(async move {
        let heads = self.head_operations.iter()
            .filter(|head| head.head_id() == &head_id)
            .map(|head| head.clone())
            .collect();

        Ok(heads)
    })}

    fn erase_head_operation<'a>(&'a mut self, id: Uuid) -> Self::HeadFuture<'a> {
    Box::pin(async move {
        let index = match self.head_operations.binary_search_by_key(&id, |h| *h.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a head operation with id '{id}'")
            )),
        };

        let head_operation = self.head_operations.remove(index);
        Ok(head_operation)
    })}

    fn save_item_operation<'a>(&'a mut self, operation: ItemOperation) -> Self::EmtpyFuture<'a> {
    Box::pin(async move {
        let index = self.item_operations.binary_search_by_key(operation.id(), |h| *h.id())
            .unwrap_or_else(|i| i);
        self.item_operations.insert(index, operation.clone());

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut InMemoryStorage| {
                store.item_operations.remove(index);
                Ok(())
            }));
        }

        Ok(())
    })}

    fn load_all_item_operations<'a>(&'a mut self) -> Self::VecItemFuture<'a> {
    Box::pin(async move {
        Ok(self.item_operations.clone())
    })}

    fn load_all_associated_item_operations<'a>(&'a mut self, item_id: Uuid) -> Self::VecItemFuture<'a> {
    Box::pin(async move {
        let items = self.item_operations.iter()
            .filter(|item| item.item_id() == &item_id)
            .map(|item| item.clone())
            .collect();

        Ok(items)
    })}

    fn erase_item_operation<'a>(&'a mut self, id: Uuid) -> Self::ItemFuture<'a> {
    Box::pin(async move {
        let index = match self.item_operations.binary_search_by_key(&id, |i| *i.id()) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("storage does not contain a item operation with id '{id}'")
            )),
        };

        let item_operation = self.item_operations.remove(index);
        Ok(item_operation)
    })}
}
