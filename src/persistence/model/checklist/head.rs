use itc::EventTree;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
pub enum HeadOperation {
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

impl HeadOperation {
    pub fn id(&self) -> &Uuid {
        match self {
            HeadOperation::Creation { id, .. } => id,
            HeadOperation::NameUpdate { id, .. } => id,
            HeadOperation::DescriptionUpdate { id, .. } => id,
            HeadOperation::CompletedUpdate { id, .. } => id,
            HeadOperation::Deletion { id, .. } => id,
        }
    }

    pub fn itc_event(&self) -> &EventTree {
        match self {
            HeadOperation::Creation { itc_event, .. } => itc_event,
            HeadOperation::NameUpdate { itc_event, .. } => itc_event,
            HeadOperation::DescriptionUpdate { itc_event, .. } => itc_event,
            HeadOperation::CompletedUpdate { itc_event, .. } => itc_event,
            HeadOperation::Deletion { itc_event, .. } => itc_event,
        }
    }

    pub fn head_id(&self) -> &Uuid {
        match self {
            HeadOperation::Creation { id, .. } => id,
            HeadOperation::NameUpdate { head_id, .. } => head_id,
            HeadOperation::DescriptionUpdate { head_id, .. } => head_id,
            HeadOperation::CompletedUpdate { head_id, .. } => head_id,
            HeadOperation::Deletion { head_id, .. } => head_id,
        }
    }
}
