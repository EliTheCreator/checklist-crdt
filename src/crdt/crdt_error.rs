
use core::error::Error;
use core::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum CrdtError {
    Recovered(String),
    Recoverable(String),
    Fatal(String),
}

impl Display for CrdtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            CrdtError::Recovered(s) => write!(f, "recovered crdt error: {}", s),
            CrdtError::Recoverable(s) => write!(f, "recoverable crdt error: {}", s),
            CrdtError::Fatal(s) => write!(f, "fatal crdt error: {}", s),
        }
    }
}

impl Error for CrdtError {}
