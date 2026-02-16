use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;


#[derive(Clone)]
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
        name: String,
    },
    PositionUpdate {
        id: Uuid,
        itc_event: EventTree,
        position: FractionalIndex,
    },
    CheckedUpdate {
        id: Uuid,
        itc_event: EventTree,
        checked: bool,
    },
    Deletion {
        id: Uuid,
        itc_event: EventTree,
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
}
