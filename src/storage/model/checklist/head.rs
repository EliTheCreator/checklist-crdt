use itc::EventTree;
use uuid::Uuid;

pub struct HeadEventMeta {
    pub id: Uuid,
    pub head_id: Uuid,
    pub itc_event: EventTree,
}

pub enum ChecklistHeadEvent {
    Creation {
        meta: HeadEventMeta,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>,
    },
    NameUpdate {
        meta: HeadEventMeta,
        name: String,
    },
    DescriptionUpdate {
        meta: HeadEventMeta,
        description: String,
    },
    CompletedUpdate {
        meta: HeadEventMeta,
        completed: bool,
    },
    Deletion {
        meta: HeadEventMeta,
    },
}
