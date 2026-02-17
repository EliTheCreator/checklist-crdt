use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum ErrorKind {
    NoStamp,
    InvalidStamp,
}


#[derive(Debug)]
pub struct StampError {
    pub kind: ErrorKind,
    pub message: String,
}

impl StampError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }
    pub fn no_stamp(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NoStamp, message.into())
    }

    pub fn invalid_stamp(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidStamp, message.into())
    }
}

impl Display for StampError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use self::ErrorKind::*;
        match self.kind {
            NoStamp => write!(f, "no stamp error: {}", self.message),
            InvalidStamp => write!(f, "invalid stamp error: {}", self.message),
        }
    }
}

impl Error for StampError {}
