use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
pub enum ItemEvent {
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

impl ItemEvent {
    pub fn id(&self) -> &Uuid {
        match self {
            ItemEvent::Creation { id, .. } => id,
            ItemEvent::NameUpdate { id, .. } => id,
            ItemEvent::PositionUpdate { id, .. } => id,
            ItemEvent::CheckedUpdate { id, .. } => id,
            ItemEvent::Deletion { id, .. } => id,
        }
    }

    pub fn itc_event(&self) -> &EventTree {
        match self {
            ItemEvent::Creation { itc_event, .. } => itc_event,
            ItemEvent::NameUpdate { itc_event, .. } => itc_event,
            ItemEvent::PositionUpdate { itc_event, .. } => itc_event,
            ItemEvent::CheckedUpdate { itc_event, .. } => itc_event,
            ItemEvent::Deletion { itc_event, .. } => itc_event,
        }
    }

    pub fn item_id(&self) -> &Uuid {
        match self {
            ItemEvent::Creation { id, .. } => id,
            ItemEvent::NameUpdate { item_id, .. } => item_id,
            ItemEvent::PositionUpdate { item_id, .. } => item_id,
            ItemEvent::CheckedUpdate { item_id, .. } => item_id,
            ItemEvent::Deletion { item_id, .. } => item_id,
        }
    }
}
