use core::error::Error;
use core::fmt::{Display, Formatter};
use loro_fractional_index::FractionalIndex;

use exn::{bail, Result};
use itc::EventTree;
use uuid::Uuid;

use super::Operation;


pub struct TombstoneBuilder {
    id: Uuid,
    history: EventTree,
    pub item_id: Uuid,
    head_id: Option<Uuid>,
    name: Option<String>,
    position: Option<FractionalIndex>,
    checked: Option<bool>,
}

impl TombstoneBuilder {
    pub fn new(id: Uuid, history: EventTree, item_id: &Uuid) -> Self {
        Self {
            id,
            history,
            item_id: item_id.clone(),
            head_id: None,
            name: None,
            position: None,
            checked: None,
        }
    }

    pub fn new_from(
        id: Uuid,
        history: EventTree,
        operation: &Operation
    ) -> Result<Self, TombstoneBuilderError> {
        let builder = Self::new(id, history, operation.item_id());
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &Operation) -> Result<Self, TombstoneBuilderError> {
        if &self.item_id != operation.item_id() {
            bail!(TombstoneBuilderError::item_id_mismatch(format!(
                "item id '{}' does not match expected item id '{}'",
                operation.item_id(),
                self.item_id,
            )))
        }

        use Operation::*;
        match operation {
            Creation { head_id, name, position, .. } => {
                self.head_id.get_or_insert(head_id.clone());
                self.name.get_or_insert(name.clone());
                self.position.get_or_insert(position.clone());
            },
            NameUpdate { name, .. } => { self.name = Some(name.clone()) },
            PositionUpdate { position, .. } => { self.position = Some(position.clone()) },
            CheckedUpdate { checked, .. } => { self.checked = Some(checked.clone()) },
            Deletion { .. } => (),
        };

        Ok(self)
    }

    pub fn build(self) -> Result<Tombstone, TombstoneBuilderError> {
        Ok(Tombstone {
            id: self.id,
            history: self.history,
            item_id: self.item_id,
            head_id: self.head_id.ok_or(TombstoneBuilderError::missing_field("head_id"))?,
            name: self.name.ok_or(TombstoneBuilderError::missing_field("name"))?,
            position: self.position.ok_or(TombstoneBuilderError::missing_field("completed"))?,
            checked: self.checked.ok_or(TombstoneBuilderError::missing_field("checked"))?,
        })
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    ItemIdMismatch,
    MissingField,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneBuilderError {
    pub kind: ErrorKind,
    pub message: String,
}

impl TombstoneBuilderError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn item_id_mismatch(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::ItemIdMismatch, message: message.into() }
    }

    pub fn missing_field(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::MissingField, message: message.into() }
    }
}

impl Display for TombstoneBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        use self::ErrorKind::*;
        match self.kind {
            ItemIdMismatch => {
                write!(f, "haed tombstone builder error: {}", self.message)
            },
            MissingField => {
                write!(f, "item tombstone builder error: missing value for '{}' field", self.message)
            },
        }
    }
}

impl Error for TombstoneBuilderError {}

#[derive(Clone, Debug)]
pub struct Tombstone {
    pub id: Uuid,
    pub history: EventTree,
    pub head_id: Uuid,
    pub item_id: Uuid,
    pub name: String,
    pub position: FractionalIndex,
    pub checked: bool,
}
