use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum ErrorKind {
    PartialRollback,
    FullRollback,
}


#[derive(Debug)]
pub struct TransactionError {
    pub kind: ErrorKind,
    pub message: String,
}

impl TransactionError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
    pub fn partial_rollback(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::PartialRollback, message.into())
    }

    pub fn full_rollback(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::FullRollback, message.into())
    }
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use self::ErrorKind::*;
        match self.kind {
            PartialRollback => write!(f, "transaction partial rollback error: {}", self.message),
            FullRollback => write!(f, "transaction full rollback error: {}", self.message),
        }
    }
}

impl Error for TransactionError {}
