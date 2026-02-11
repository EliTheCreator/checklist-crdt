
use core::error::Error;
use core::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub struct StorageError(pub String);

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "storage error: {}", self.0)
    }
}

impl Error for StorageError {}
