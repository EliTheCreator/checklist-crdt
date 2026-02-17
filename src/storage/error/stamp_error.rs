use core::error::Error;
use core::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum StampError {
    NoStamp(String),
    InvalidStamp(String),
}

impl Display for StampError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            StampError::NoStamp(s) => write!(f, "no stamp error: {}", s),
            StampError::InvalidStamp(s) => write!(f, "invalid stamp error: {}", s),
        }
    }
}

impl Error for StampError {}
