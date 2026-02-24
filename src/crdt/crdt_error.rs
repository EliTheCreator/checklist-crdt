use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Recovered,
    Recoverable,
    Fatal,
    CausalGap,
}


#[derive(Debug, PartialEq, Eq)]
pub struct CrdtError {
    kind: ErrorKind,
    message: String
}

impl CrdtError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn recovered(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Recovered, message: message.into() }
    }

    pub fn recoverable(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Recoverable, message: message.into() }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Recoverable, message: message.into() }
    }

    pub fn causal_gap(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::CausalGap, message: message.into() }
    }
}

impl Display for CrdtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use self::ErrorKind::*;
        match self.kind {
            Recovered => write!(f, "crdt recovered error: {}", self.message),
            Recoverable => write!(f, "crdt recoverable error: {}", self.message),
            Fatal => write!(f, "crdt fatal error: {}", self.message),
            CausalGap => write!(f, "crdt causal gap error: {}", self.message),
        }
    }
}

impl Error for CrdtError {}
