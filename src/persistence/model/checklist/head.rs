use itc::EventTree;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
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
        itc_event: EventTree,
        head_id: Uuid,
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

    pub fn itc_event(&self) -> &EventTree {
        match self {
            HeadEvent::Creation { itc_event, .. } => itc_event,
            HeadEvent::NameUpdate { itc_event, .. } => itc_event,
            HeadEvent::DescriptionUpdate { itc_event, .. } => itc_event,
            HeadEvent::CompletedUpdate { itc_event, .. } => itc_event,
            HeadEvent::Deletion { itc_event, .. } => itc_event,
        }
    }

    pub fn head_id(&self) -> &Uuid {
        match self {
            HeadEvent::Creation { id, .. } => id,
            HeadEvent::NameUpdate { head_id, .. } => head_id,
            HeadEvent::DescriptionUpdate { head_id, .. } => head_id,
            HeadEvent::CompletedUpdate { head_id, .. } => head_id,
            HeadEvent::Deletion { head_id, .. } => head_id,
        }
    }
}
