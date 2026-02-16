use itc::EventTree;
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;


#[derive(Clone)]
pub struct ItemEventMeta {
    pub id: Uuid,
    pub item_id: Uuid,
    pub itc_event: EventTree,
}


#[derive(Clone)]
pub enum ChecklistItemEvent {
    Creation {
        meta: ItemEventMeta,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    },
    NameUpdate {
        meta: ItemEventMeta,
        name: String,
    },
    PositionUpdate {
        meta: ItemEventMeta,
        position: FractionalIndex,
    },
    CheckedUpdate {
        meta: ItemEventMeta,
        checked: bool,
    },
    Deletion {
        meta: ItemEventMeta,
    },
}