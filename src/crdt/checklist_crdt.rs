use exn::{bail, Exn, Result, ResultExt};
use itc::{IntervalTreeClock, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::crdt::crdt_error::CrdtError;
use crate::storage::model::checklist::head::HeadEvent;
use crate::storage::model::checklist::item::ItemEvent;
use crate::storage::storage_error::{StorageError, ErrorKind};
use crate::storage::store::Store;
use crate::transport::transport::Transport;


pub struct ChecklistCrdt<S: Store, T: Transport> {
    storage: S,
    transport: T,
    itc_stamp: Stamp
}

impl<S: Store, T: Transport> ChecklistCrdt<S, T> {
    pub fn new(mut storage: S, transport: T) -> Result<Self, CrdtError> {
        let stamp = match storage.load_stamp() {
            Ok(s) => s,
            Err(e) if e.kind == ErrorKind::StampNone =>{
                let stamp = Stamp::seed();
                storage.save_stamp(&stamp)
                    .or_raise(|| CrdtError::fatal("failed to save new stamp"))?;
                stamp
            },
            Err(_) => bail!(CrdtError::fatal("")),
        };

        Ok(ChecklistCrdt {
            storage: storage,
            transport: transport,
            itc_stamp: stamp,
        })
    }

    fn abort_transaction(
        &mut self,
        e1: Exn<StorageError>,
        text: &str,
    ) -> Exn<CrdtError> {
        let success_text = format!("{text}. all operations have been reverted");
        let failure_text = format!("{text}. unable to reverse all operations. crdt is in inconsistent state");
        match self.storage.abort_transaction() {
            Ok(_) => {
                e1.raise(CrdtError::recovered(success_text))
            },
            Err(e2) => {
                Exn::raise_all(CrdtError::fatal(failure_text), vec![e1, e2])
            },
        }
    }

    fn save_checklist_head_event(
        &mut self,
        stamp: Stamp,
        event: HeadEvent,
    ) -> Result<(), CrdtError> {
        self.storage.start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        self.storage.save_head_event(&event).map_err(|e|
            self.abort_transaction(e, "crdt unable to store head event")
        )?;

        let _ = self.storage.save_stamp(&stamp).map_err(|e|
            self.abort_transaction(e, "crdt unable to save stamp")
        )?;

        let _ = self.storage.commit_transaction().map_err(|e|
            self.abort_transaction(e, "crdt unable commit transaction")
        )?;

        Ok(())
    }

    fn erase_checklist_head(
        &mut self,
        id: &Uuid,
    ) -> Result<HeadEvent, CrdtError> {
        let stamp = self.itc_stamp.event();

        self.storage.start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        let event = self.storage.delete_head_event(id).map_err(|e|
            self.abort_transaction(e, "crdt unable to store head event")
        )?;

        let _ = self.storage.save_stamp(&stamp).map_err(|e|
            self.abort_transaction(e, "crdt unable to save stamp")
        )?;

        let _ = self.storage.commit_transaction().map_err(|e|
            self.abort_transaction(e, "crdt unable commit transaction")
        )?;

        Ok(event)
    }

    pub fn add_checklist_head(
        &mut self,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let event = HeadEvent::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            template_id: template_id,
            name: name,
            description: description,
        };

        self.save_checklist_head_event(stamp, event)?;
        Ok(id)
    }

    pub fn update_checklist_head_name(
        &mut self,
        head_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = HeadEvent::NameUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            name: name,
        };

        self.save_checklist_head_event(stamp, event)
    }

    pub fn update_checklist_head_description(
        &mut self,
        head_id: &Uuid,
        description: Option<String>,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = HeadEvent::DescriptionUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            description: description,
        };

        self.save_checklist_head_event(stamp, event)
    }

    pub fn update_checklist_head_completed(
        &mut self,
        head_id: &Uuid,
        completed: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = HeadEvent::CompletedUpdate {
            id: Uuid::now_v7(),
            head_id: head_id.clone(),
            itc_event: stamp.event_tree(),
            completed: completed,
        };

        self.save_checklist_head_event(stamp, event)
    }

    pub fn delete_checklist_head(
        &mut self,
        head_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = HeadEvent::Deletion {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            head_id: head_id.clone(),
        };

        self.save_checklist_head_event(stamp, event)
    }

    fn save_checklist_item_event(
        &mut self,
        stamp: Stamp,
        event: ItemEvent,
    ) -> Result<(), CrdtError> {
        self.storage.start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        self.storage.save_item_event(&event).map_err(|e|
            self.abort_transaction(e, "crdt unable to store item event")
        )?;

        let _ = self.storage.save_stamp(&stamp).map_err(|e|
            self.abort_transaction(e, "crdt unable to save stamp")
        )?;

        let _ = self.storage.commit_transaction().map_err(|e|
            self.abort_transaction(e, "crdt unable commit transaction")
        )?;

        Ok(())
    }

    fn erase_checklist_item(
        &mut self,
        id: &Uuid,
    ) -> Result<ItemEvent, CrdtError> {
        let stamp = self.itc_stamp.event();

        self.storage.start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        let event = self.storage.delete_item_event(id).map_err(|e|
            self.abort_transaction(e, "crdt unable to store item event")
        )?;

        let _ = self.storage.save_stamp(&stamp).map_err(|e|
            self.abort_transaction(e, "crdt unable to save stamp")
        )?;

        let _ = self.storage.commit_transaction().map_err(|e|
            self.abort_transaction(e, "crdt unable commit transaction")
        )?;

        Ok(event)
    }

    pub fn add_checklist_item(
        &mut self,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let event = ItemEvent::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            head_id: head_id,
            name: name,
            position: position,
        };

        self.save_checklist_item_event(stamp, event)?;
        Ok(id)
    }

    pub fn update_checklist_item_name(
        &mut self,
        item_id: &Uuid,
        name: String,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = ItemEvent::NameUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            name: name,
        };

        self.save_checklist_item_event(stamp, event)
    }

    pub fn update_checklist_item_position(
        &mut self,
        item_id: &Uuid,
        position: FractionalIndex,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = ItemEvent::PositionUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            position: position,
        };

        self.save_checklist_item_event(stamp, event)
    }

    pub fn update_checklist_item_checked(
        &mut self,
        item_id: &Uuid,
        checked: bool,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event = ItemEvent::CheckedUpdate {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
            checked: checked,
        };

        self.save_checklist_item_event(stamp, event)
    }

    pub fn delete_checklist_item(
        &mut self,
        item_id: &Uuid,
    ) -> Result<(), CrdtError> {
        let stamp = self.itc_stamp.event();
        let event: ItemEvent = ItemEvent::Deletion {
            id: Uuid::now_v7(),
            itc_event: stamp.event_tree(),
            item_id: item_id.clone(),
        };

        self.save_checklist_item_event(stamp, event)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn init_test() {
        use crate::storage::file_storage::FileStorage;

        struct DummyTransport {}
        impl Transport for DummyTransport {}

        let stamp_path = "./stamp.txt";
        let head_log_path = "./head_log.txt";
        let item_log_path = "./item_log.txt";
        let file_store = FileStorage::new(
            stamp_path,
            head_log_path,
            item_log_path,
        ).unwrap();

        let transport = DummyTransport {};

        let mut crdt = ChecklistCrdt::new(file_store, transport).unwrap();
        let index = FractionalIndex::default();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test item".into(),
            FractionalIndex::new_after(&index),
        ).unwrap();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test 4 item".into(),
            FractionalIndex::new_after(&index),
        ).unwrap();
        crdt.add_checklist_item(
            Uuid::now_v7(),
            "test 3 asdf item".into(),
            FractionalIndex::new_before(&index),
        ).unwrap();
    }
}
