use core::error::Error;
use core::fmt::{Display, Formatter, Result};


#[derive(Debug)]
pub enum BackendError {
    Open(String),
    Read(String),
    Write(String),
}

impl Display for BackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            BackendError::Open(s) => write!(f, "backend open error: {}", s),
            BackendError::Read(s) => write!(f, "backend read error: {}", s),
            BackendError::Write(s) => write!(f, "backend write error: {}", s),
        }
    }
}

impl Error for BackendError {}
