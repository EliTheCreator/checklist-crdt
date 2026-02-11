use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;



struct Checklist {
    id: Uuid,
    template_id: Option<Uuid>,
    itc_event: EventTree,
    name: String,
    description: Option<String>,
    completed: bool,
    deleted: bool,
}

struct ChecklistItem {
    id: Uuid,
    checklist_id: Uuid,
    itc_event: EventTree,
    name: String,
    position: FractionalIndex,
    checked: bool,
}


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


pub struct ItemEventMeta {
    id: Uuid,
    item_id: Uuid,
    itc_event: EventTree,
}

pub enum ChecklistItemEvent {
    Creation {
        meta: ItemEventMeta,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
        checked: bool,
    },
    NameUpdate {
        meta: ItemEventMeta,
        name: String,
    },
    PositionUpdate {
        meta: ItemEventMeta,
        position: String,
    },
    CheckedUpdate {
        meta: ItemEventMeta,
        checked: bool,
    },
    Deletion {
        meta: ItemEventMeta,
    },
}
