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
        head_id: Uuid,
        name: String,
    },
    DescriptionUpdate {
        id: Uuid,
        itc_event: EventTree,
        head_id: Uuid,
        description: Option<String>,
    },
    CompletedUpdate {
        id: Uuid,
        itc_event: EventTree,
        head_id: Uuid,
        completed: bool,
    },
    Deletion {
        id: Uuid,
        head_id: Uuid,
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

impl HeadEvent {
    pub fn itc_event(&self) -> &EventTree {
        match self {
            HeadEvent::Creation { itc_event, .. } => itc_event,
            HeadEvent::NameUpdate { itc_event, .. } => itc_event,
            HeadEvent::DescriptionUpdate { itc_event, .. } => itc_event,
            HeadEvent::CompletedUpdate { itc_event, .. } => itc_event,
            HeadEvent::Deletion { itc_event, .. } => itc_event,
        }
    }
}
