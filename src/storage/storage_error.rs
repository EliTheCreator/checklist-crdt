use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum ErrorKind {
    BackendOpen,
    BackendRead,
    BackendWrite,
    BackendSpecific,
    DataDecode,
    DataEncode,
    StampNone,
    StampInvalid,
    TransactionRollbackPartial,
    TransactionRollbackFull,
}


#[derive(Debug)]
pub struct StorageError {
    pub kind: ErrorKind,
    pub message: String,
}

impl StorageError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn backend_open(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::BackendOpen, message: message.into() }
    }

    pub fn backend_read(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::BackendRead, message: message.into() }
    }

    pub fn backend_write(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::BackendWrite, message: message.into() }
    }

    pub fn backend_specific(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::BackendSpecific, message: message.into() }
    }

    pub fn data_decode(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::DataDecode, message: message.into() }
    }

    pub fn data_encode(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::DataEncode, message: message.into() }
    }

    pub fn stamp_none(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::StampNone, message: message.into() }
    }

    pub fn stamp_invalid(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::StampInvalid, message: message.into() }
    }

    pub fn transaction_rollback_partial(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::TransactionRollbackPartial, message: message.into() }
    }

    pub fn transaction_rollback_full(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::TransactionRollbackFull, message: message.into() }
    }
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use self::ErrorKind::*;
        match self.kind {
            BackendOpen => write!(f, "backend open error: {}", self.message),
            BackendRead => write!(f, "backend read error: {}", self.message),
            BackendWrite => write!(f, "backend write error: {}", self.message),
            BackendSpecific => write!(f, "backend specific error: {}", self.message),
            DataDecode => write!(f, "data decode error: {}", self.message),
            DataEncode => write!(f, "data encode error: {}", self.message),
            StampNone => write!(f, "stamp missing error: {}", self.message),
            StampInvalid => write!(f, "stamp invalid error: {}", self.message),
            TransactionRollbackPartial => write!(f, "transaction rollback partial error: {}", self.message),
            TransactionRollbackFull => write!(f, "transaction rollback full error: {}", self.message),
        }
    }
}

impl Error for StorageError {}
