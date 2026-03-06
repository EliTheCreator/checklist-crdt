use core::error::Error;
use core::fmt::{Display, Formatter};

use exn::{bail, Result};
use itc::EventTree;
use uuid::Uuid;

use super::Operation;


pub struct TombstoneBuilder {
    id: Uuid,
    history: EventTree,
    pub head_id: Uuid,
    template_id: Option<Uuid>,
    name: Option<String>,
    description: Option<String>,
    completed: Option<bool>,
}

impl TombstoneBuilder {
    pub fn new(id: Uuid, history: EventTree, head_id: &Uuid) -> Self {
        Self {
            id,
            history,
            head_id: head_id.clone(),
            template_id: None,
            name: None,
            description: None,
            completed: None,
        }
    }

    pub fn new_from(
        id: Uuid,
        history: EventTree,
        operation: &Operation
    ) -> Result<Self, TombstoneBuilderError> {
        let builder = Self::new(id, history, operation.head_id());
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &Operation) -> Result<Self, TombstoneBuilderError> {
        if &self.head_id != operation.head_id() {
            bail!(TombstoneBuilderError::head_id_mismatch(format!(
                "head id '{}' does not match expected head id '{}'",
                operation.head_id(),
                self.head_id,
            )))
        }

        use Operation::*;
        match operation {
            Creation { template_id, name, description, .. } => {
                self.template_id = self.template_id.or(template_id.clone());
                self.name.get_or_insert(name.clone());
                if self.description.is_none() {
                    self.description = description.clone();
                }
            },
            NameUpdate { name, ..  } => { self.name = Some(name.clone()) },
            DescriptionUpdate { description, .. } => { self.description = description.clone() },
            CompletedUpdate { completed, .. } => { self.completed = Some(completed.clone()) },
        };

        Ok(self)
    }

    pub fn build(self) -> Result<Tombstone, TombstoneBuilderError> {
        Ok(Tombstone {
            id: self.id,
            history: self.history,
            head_id: self.head_id,
            template_id: self.template_id,
            name: self.name.ok_or(TombstoneBuilderError::missing_field("name"))?,
            description: self.description,
            completed: self.completed.ok_or(TombstoneBuilderError::missing_field("completed"))?,
        })
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    HeadIdMismatch,
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

    pub fn head_id_mismatch(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::HeadIdMismatch, message: message.into() }
    }

    pub fn missing_field(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::MissingField, message: message.into() }
    }
}

impl Display for TombstoneBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        use self::ErrorKind::*;
        match self.kind {
            HeadIdMismatch => {
                write!(f, "head tombstone builder error: {}", self.message)
            },
            MissingField => {
                write!(f, "head tombstone builder error: missing value for '{}' field", self.message)
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
    pub template_id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub completed: bool,
}
