use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
pub enum ItemOperation {
    Creation {
        id: Uuid,
        itc_event: EventTree,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    },
    NameUpdate {
        id: Uuid,
        itc_event: EventTree,
        item_id: Uuid,
        name: String,
    },
    PositionUpdate {
        id: Uuid,
        itc_event: EventTree,
        item_id: Uuid,
        position: FractionalIndex,
    },
    CheckedUpdate {
        id: Uuid,
        itc_event: EventTree,
        item_id: Uuid,
        checked: bool,
    },
    Deletion {
        id: Uuid,
        itc_event: EventTree,
        item_id: Uuid,
    },
}

impl ItemOperation {
    pub fn id(&self) -> &Uuid {
        match self {
            ItemOperation::Creation { id, .. } => id,
            ItemOperation::NameUpdate { id, .. } => id,
            ItemOperation::PositionUpdate { id, .. } => id,
            ItemOperation::CheckedUpdate { id, .. } => id,
            ItemOperation::Deletion { id, .. } => id,
        }
    }

    pub fn itc_event(&self) -> &EventTree {
        match self {
            ItemOperation::Creation { itc_event, .. } => itc_event,
            ItemOperation::NameUpdate { itc_event, .. } => itc_event,
            ItemOperation::PositionUpdate { itc_event, .. } => itc_event,
            ItemOperation::CheckedUpdate { itc_event, .. } => itc_event,
            ItemOperation::Deletion { itc_event, .. } => itc_event,
        }
    }

    pub fn item_id(&self) -> &Uuid {
        match self {
            ItemOperation::Creation { id, .. } => id,
            ItemOperation::NameUpdate { item_id, .. } => item_id,
            ItemOperation::PositionUpdate { item_id, .. } => item_id,
            ItemOperation::CheckedUpdate { item_id, .. } => item_id,
            ItemOperation::Deletion { item_id, .. } => item_id,
        }
    }
}
