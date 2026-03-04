use core::error::Error;
use core::fmt::{Display, Formatter};

use itc::EventTree;
use uuid::Uuid;


pub struct HeadOperationTombstoneBuilder {
    id: Uuid,
    history: EventTree,
    head_id: Option<Uuid>,
    template_id: Option<Uuid>,
    name: Option<String>,
    description: Option<String>,
    completed: Option<bool>,
}

impl HeadOperationTombstoneBuilder {
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

    pub fn new_from(id: Uuid, history: EventTree, operation: &HeadOperation) -> Self {
        let builder = Self::new(id, history);
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &HeadOperation) -> Self {
        use HeadOperation::*;
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
            Tombstone { .. } => todo!(),
        };

        self
    }

    pub fn build(self) -> Result<HeadOperation, HeadTombstoneBuilderError> {
        Ok(HeadOperation::Tombstone {
            id: self.id,
            history: self.history,
            head_id: self.head_id.ok_or(HeadTombstoneBuilderError::new("head_id"))?,
            template_id: self.template_id,
            name: self.name.ok_or(HeadTombstoneBuilderError::new("name"))?,
            description: self.description,
            completed: self.completed.ok_or(HeadTombstoneBuilderError::new("completed"))?,
        })
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct HeadTombstoneBuilderError {
    missing_field: String,
}

impl HeadTombstoneBuilderError {
    pub fn new(missing_field: impl Into<String>) -> Self {
        Self { missing_field: missing_field.into() }
    }
}

impl Display for HeadTombstoneBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "tombstone builder error: missing value for {} field", self.missing_field)
    }
}

impl Error for HeadTombstoneBuilderError {}


#[derive(Debug, Clone, PartialEq)]
pub enum HeadOperation {
    Creation {
        id: Uuid,
        history: EventTree,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
    },
    NameUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        name: String,
    },
    DescriptionUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        description: Option<String>,
    },
    CompletedUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        completed: bool,
    },
    Deletion {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
    },
    Tombstone {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
        completed: bool,
    },
}

impl HeadOperation {
    pub fn id(&self) -> &Uuid {
        use HeadOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { id, .. } => id,
            DescriptionUpdate { id, .. } => id,
            CompletedUpdate { id, .. } => id,
            Deletion { id, .. } => id,
            Tombstone { id, .. } => id,
        }
    }

    pub fn history(&self) -> &EventTree {
        use HeadOperation::*;
        match self {
            Creation { history, .. } => history,
            NameUpdate { history, .. } => history,
            DescriptionUpdate { history, .. } => history,
            CompletedUpdate { history, .. } => history,
            Deletion { history, .. } => history,
            Tombstone { history, .. } => history,
        }
    }

    pub fn head_id(&self) -> &Uuid {
        use HeadOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { head_id, .. } => head_id,
            DescriptionUpdate { head_id, .. } => head_id,
            CompletedUpdate { head_id, .. } => head_id,
            Deletion { head_id, .. } => head_id,
            Tombstone { head_id, .. } => head_id,
        }
    }
}
