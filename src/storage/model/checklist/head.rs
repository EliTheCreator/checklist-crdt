use itc::EventTree;
use uuid::Uuid;


#[derive(Clone)]
pub enum HeadEvent {
    Creation {
        id: Uuid,
        itc_event: EventTree,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
    },
    NameUpdate {
        id: Uuid,
        itc_event: EventTree,
        name: String,
    },
    DescriptionUpdate {
        id: Uuid,
        itc_event: EventTree,
        description: String,
    },
    CompletedUpdate {
        id: Uuid,
        itc_event: EventTree,
        completed: bool,
    },
    Deletion {
        id: Uuid,
        itc_event: EventTree,
    },
}

impl HeadEvent {
    pub fn id(&self) -> &Uuid {
        match self {
            HeadEvent::Creation { id, .. } => id,
            HeadEvent::NameUpdate { id, .. } => id,
            HeadEvent::DescriptionUpdate { id, .. } => id,
            HeadEvent::CompletedUpdate { id, .. } => id,
            HeadEvent::Deletion { id, .. } => id,
        }
    }
}
