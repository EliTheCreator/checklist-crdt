use core::error::Error;
use core::fmt::{Display, Formatter, Result};

use super::backend_error::BackendError;
use super::stamp_error::StampError;
use super::transaction_error::TransactionError;


#[derive(Debug)]
pub enum StorageError {
    Stamp(StampError),
    Transaction(TransactionError),
    Backend(BackendError),
}

impl StorageError {
    pub fn stamp(stamp_error: StampError) -> Self {
        Self::Stamp(stamp_error)
    }

    pub fn transaction(transaction_error: TransactionError) -> Self {
        Self::Transaction(transaction_error)
    }

    pub fn backend(backend_error: BackendError) -> Self {
        Self::Backend(backend_error)
    }
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            StorageError::Stamp(stamp_error) => stamp_error.fmt(f),
            StorageError::Transaction(transaction_error) => transaction_error.fmt(f),
            StorageError::Backend(backend_error) => backend_error.fmt(f),
        }
    }
}

impl Error for StorageError {}
