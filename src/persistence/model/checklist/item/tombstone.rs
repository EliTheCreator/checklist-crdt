use core::error::Error;
use core::fmt::{Display, Formatter};
use loro_fractional_index::FractionalIndex;

use itc::EventTree;
use uuid::Uuid;

use super::ItemOperation;


pub struct ItemOperationTombstoneBuilder {
    id: Uuid,
    history: EventTree,
    item_id: Option<Uuid>,
    head_id: Option<Uuid>,
    name: Option<String>,
    position: Option<FractionalIndex>,
    checked: Option<bool>,
}

impl ItemOperationTombstoneBuilder {
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

    pub fn new_from(id: Uuid, history: EventTree, operation: &ItemOperation) -> Self {
        let builder = Self::new(id, history);
        builder.apply(operation)
    }

    pub fn apply(mut self, operation: &ItemOperation) -> Self {
        use ItemOperation::*;
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
            Tombstone { .. } => todo!(),
        };

        self
    }

    pub fn build(self) -> Result<ItemOperation, ItemTombstoneBuilderError> {
        Ok(ItemOperation::Tombstone {
            id: self.id,
            history: self.history,
            item_id: self.item_id.ok_or(ItemTombstoneBuilderError::new("item_id"))?,
            head_id: self.head_id.ok_or(ItemTombstoneBuilderError::new("head_id"))?,
            name: self.name.ok_or(ItemTombstoneBuilderError::new("name"))?,
            position: self.position.ok_or(ItemTombstoneBuilderError::new("completed"))?,
            checked: self.checked.ok_or(ItemTombstoneBuilderError::new("checked"))?,
        })
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct ItemTombstoneBuilderError {
    missing_field: String,
}

impl ItemTombstoneBuilderError {
    pub fn new(missing_field: impl Into<String>) -> Self {
        Self { missing_field: missing_field.into() }
    }
}

impl Display for ItemTombstoneBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "tombstone builder error: missing value for {} field", self.missing_field)
    }
}

impl Error for ItemTombstoneBuilderError {}
