use itc::EventTree;
use uuid::Uuid;


#[derive(Debug, Clone, PartialEq)]
pub enum HeadOperation {
    Creation {
        id: Uuid,
        history: EventTree,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
    },
    NameUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        name: String,
    },
    DescriptionUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        description: Option<String>,
    },
    CompletedUpdate {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        completed: bool,
    },
    Deletion {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
    },
    Tombstone {
        id: Uuid,
        history: EventTree,
        head_id: Uuid,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
        completed: bool,
    },
}

impl HeadOperation {
    pub fn id(&self) -> &Uuid {
        use HeadOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { id, .. } => id,
            DescriptionUpdate { id, .. } => id,
            CompletedUpdate { id, .. } => id,
            Deletion { id, .. } => id,
            Tombstone { id, .. } => id,
        }
    }

    pub fn history(&self) -> &EventTree {
        use HeadOperation::*;
        match self {
            Creation { history, .. } => history,
            NameUpdate { history, .. } => history,
            DescriptionUpdate { history, .. } => history,
            CompletedUpdate { history, .. } => history,
            Deletion { history, .. } => history,
            Tombstone { history, .. } => history,
        }
    }

    pub fn head_id(&self) -> &Uuid {
        use HeadOperation::*;
        match self {
            Creation { id, .. } => id,
            NameUpdate { head_id, .. } => head_id,
            DescriptionUpdate { head_id, .. } => head_id,
            CompletedUpdate { head_id, .. } => head_id,
            Deletion { head_id, .. } => head_id,
            Tombstone { head_id, .. } => head_id,
        }
    }
}
