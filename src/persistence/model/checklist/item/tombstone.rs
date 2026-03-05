use core::error::Error;
use core::fmt::{Display, Formatter};
use loro_fractional_index::FractionalIndex;

use itc::EventTree;
use uuid::Uuid;

use super::Operation;


pub struct TombstoneBuilder {
    id: Uuid,
    history: EventTree,
    item_id: Option<Uuid>,
    head_id: Option<Uuid>,
    name: Option<String>,
    position: Option<FractionalIndex>,
    checked: Option<bool>,
}

impl TombstoneBuilder {
    pub fn new(id: Uuid, history: EventTree) -> Self {
        Self {
            id,
            history,
            item_id: None,
            head_id: None,
            name: None,
            position: None,
            checked: None,
        }
    }

    pub fn new_from(id: Uuid, history: EventTree, operation: &Operation) -> Self {
        let builder = Self::new(id, history);
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &Operation) -> Self {
        use Operation::*;
        match operation {
            Creation { id, head_id, name, position, .. } => {
                self.head_id.get_or_insert(head_id.clone());
                self.item_id.get_or_insert(id.clone());
                self.name.get_or_insert(name.clone());
                self.position.get_or_insert(position.clone());
            },
            NameUpdate { name, .. } => { self.name = Some(name.clone()) },
            PositionUpdate { position, .. } => { self.position = Some(position.clone()) },
            CheckedUpdate { checked, .. } => { self.checked = Some(checked.clone()) },
            Deletion { .. } => (),
        };

        self
    }

    pub fn build(self) -> Result<Tombstone, TombstoneBuilderError> {
        Ok(Tombstone {
            id: self.id,
            history: self.history,
            item_id: self.item_id.ok_or(TombstoneBuilderError::new("item_id"))?,
            head_id: self.head_id.ok_or(TombstoneBuilderError::new("head_id"))?,
            name: self.name.ok_or(TombstoneBuilderError::new("name"))?,
            position: self.position.ok_or(TombstoneBuilderError::new("completed"))?,
            checked: self.checked.ok_or(TombstoneBuilderError::new("checked"))?,
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
    pub item_id: Uuid,
    pub name: String,
    pub position: FractionalIndex,
    pub checked: bool,
}
