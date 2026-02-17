use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum TransactionError {
    PartialRollback(String),
    FullRollback(String),
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            TransactionError::PartialRollback(s) => write!(f, "transaction rollback partial error: {}", s),
            TransactionError::FullRollback(s) => write!(f, "transaction rollback full error: {}", s),
        }
    }
}

impl Error for TransactionError {}
