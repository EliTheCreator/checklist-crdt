use core::error::Error;
use core::fmt::{Display, Formatter};

use itc::EventTree;
use uuid::Uuid;

use super::Operation;


pub struct TombstoneBuilder {
    id: Uuid,
    history: EventTree,
    head_id: Option<Uuid>,
    template_id: Option<Uuid>,
    name: Option<String>,
    description: Option<String>,
    completed: Option<bool>,
}

impl TombstoneBuilder {
    pub fn new(id: Uuid, history: EventTree) -> Self {
        Self {
            id,
            history,
            head_id: None,
            template_id: None,
            name: None,
            description: None,
            completed: None,
        }
    }

    pub fn new_from(id: Uuid, history: EventTree, operation: &Operation) -> Self {
        let builder = Self::new(id, history);
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &Operation) -> Self {
        use Operation::*;
        match operation {
            Creation { id, template_id, name, description, .. } => {
                self.head_id.get_or_insert(id.clone());
                self.template_id = self.template_id.or(template_id.clone());
                self.name.get_or_insert(name.clone());
                if self.description.is_none() {
                    self.description = description.clone();
                }
            },
            NameUpdate { name, ..  } => { self.name = Some(name.clone()) },
            DescriptionUpdate { description, .. } => { self.description = description.clone() },
            CompletedUpdate { completed, .. } => { self.completed = Some(completed.clone()) },
            Deletion { .. } => (),
        };

        self
    }

    pub fn build(self) -> Result<Tombstone, TombstoneBuilderError> {
        Ok(Tombstone {
            id: self.id,
            history: self.history,
            head_id: self.head_id.ok_or(TombstoneBuilderError::new("head_id"))?,
            template_id: self.template_id,
            name: self.name.ok_or(TombstoneBuilderError::new("name"))?,
            description: self.description,
            completed: self.completed.ok_or(TombstoneBuilderError::new("completed"))?,
        })
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct TombstoneBuilderError {
    missing_field: String,
}

impl TombstoneBuilderError {
    pub fn new(missing_field: impl Into<String>) -> Self {
        Self { missing_field: missing_field.into() }
    }
}

impl Display for TombstoneBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "tombstone builder error: missing value for {} field", self.missing_field)
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
