use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
pub enum ItemOperation {
    Creation {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    },
    NameUpdate {
        id: Uuid,
        history: EventTree,
        item_id: Uuid,
        name: String,
    },
    PositionUpdate {
        id: Uuid,
        history: EventTree,
        item_id: Uuid,
        position: FractionalIndex,
    },
    CheckedUpdate {
        id: Uuid,
        history: EventTree,
        item_id: Uuid,
        checked: bool,
    },
    Deletion {
        id: Uuid,
        history: EventTree,
        item_id: Uuid,
    },
    Tombstone {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        item_id: Uuid,
        name: String,
        position: FractionalIndex,
        checked: bool,
    }
}

impl ItemOperation {
    pub fn id(&self) -> &Uuid {
        use ItemOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { id, .. } => id,
            PositionUpdate { id, .. } => id,
            CheckedUpdate { id, .. } => id,
            Deletion { id, .. } => id,
            Tombstone {id, .. } => id,
        }
    }

    pub fn history(&self) -> &EventTree {
        use ItemOperation::*;
        match self {
            Creation { history, .. } => history,
            NameUpdate { history, .. } => history,
            PositionUpdate { history, .. } => history,
            CheckedUpdate { history, .. } => history,
            Deletion { history, .. } => history,
            Tombstone { history, .. } => history,
        }
    }

    pub fn item_id(&self) -> &Uuid {
        use ItemOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { item_id, .. } => item_id,
            PositionUpdate { item_id, .. } => item_id,
            CheckedUpdate { item_id, .. } => item_id,
            Deletion { item_id, .. } => item_id,
            Tombstone { item_id, .. } => item_id,
        }
    }
}
