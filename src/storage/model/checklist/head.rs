use itc::EventTree;
use uuid::Uuid;


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
            HeadEvent::Creation { id, itc_event, template_id, name, description } => id,
            HeadEvent::NameUpdate { id, itc_event, name } => id,
            HeadEvent::DescriptionUpdate { id, itc_event, description } => id,
            HeadEvent::CompletedUpdate { id, itc_event, completed } => id,
            HeadEvent::Deletion { id, itc_event } => id,
        }
    }
}
