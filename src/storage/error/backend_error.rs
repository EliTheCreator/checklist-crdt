use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum ErrorKind {
    Open,
    Read,
    Write,
}


#[derive(Debug)]
pub struct BackendError {
    pub kind: ErrorKind,
    pub message: String,
}

impl BackendError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
    pub fn open(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Open, message.into())
    }

    pub fn read(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Read, message.into())
    }

    pub fn write(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Write, message.into())
    }
}

impl Display for BackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use self::ErrorKind::*;
        match self.kind {
            Open => write!(f, "backend open error: {}", self.message),
            Read => write!(f, "backend read error: {}", self.message),
            Write => write!(f, "backend write error: {}", self.message),
        }
    }
}

impl Error for BackendError {}
