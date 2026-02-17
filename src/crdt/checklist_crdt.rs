use exn::{Exn, Result, ResultExt};
use itc::{IntervalTreeClock, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::crdt::crdt_error::CrdtError;
use crate::storage::model::checklist::head::HeadEvent;
use crate::storage::model::checklist::item::ItemEvent;
use crate::storage::storage_error::StorageError;
use crate::storage::store::Store;
use crate::transport::transport::Transport;


pub struct ChecklistCrdt<S: Store, T: Transport> {
    storage: S,
    transport: T,
    itc_stamp: Stamp
}

impl<S: Store, T: Transport> ChecklistCrdt<S, T> {
    pub fn new(storage: S, transport: T) -> Self {
        ChecklistCrdt {
            storage: storage,
            transport: transport,
            itc_stamp: Stamp::seed(),
        }
    }

    fn save_stamp_or_revert(
        &mut self,
        itc_stamp: Stamp,
        reversion_f: fn(&mut ChecklistCrdt<S, T>, &Uuid) -> Result<(), StorageError>,
        id: Uuid,
        success_text: &str,
        failure_text: &str,
    ) -> Result<Uuid, CrdtError> {
        match self.storage.save_stamp(&itc_stamp) {
            Ok(_) => {
                self.itc_stamp = itc_stamp;
                Ok(id)
            },
            Err(e1) => {
                match reversion_f(self, &id) {
                    Ok(_) => {
                        Result::Err(e1).or_raise(|| CrdtError::Recovered(success_text.into()))
                    },
                    Err(e2) => {
                        Result::Err(Exn::raise_all(CrdtError::Fatal(failure_text.into()), vec![e1, e2]))
                    },
                }
            }
        }
    }

    pub fn add_checklist_head(
        &mut self,
        template_id: Option<Uuid>,
        name: String,
        description: Option<String>
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let checklist_head_event = HeadEvent::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            template_id: template_id,
            name: name,
            description: description,
        };

        self.storage.save_head_event(&checklist_head_event)
            .or_raise(|| CrdtError::Recovered("crdt unable to add checklist head".into()))?;

        let success_text = "crdt unable to save stamp. addition of checklist head reverted";
        let failure_text = "crdt unable to save stamp. reversion of checklist head addition failed. crdt is in inconsistent state";
        let reversion_f = |crdt: &mut ChecklistCrdt<S, T>, id: &Uuid| crdt.storage.delete_head_event(id).map(|_| ());
        self.save_stamp_or_revert(stamp, reversion_f, id, success_text, failure_text)
    }

    pub fn add_checklist_item(
        &mut self,
        head_id: Uuid,
        name: String,
        position: FractionalIndex,
    ) -> Result<Uuid, CrdtError> {
        let stamp = self.itc_stamp.event();
        let id = Uuid::now_v7();
        let checklist_item_event = ItemEvent::Creation {
            id: id.clone(),
            itc_event: stamp.event_tree(),
            head_id: head_id,
            name: name,
            position: position,
        };

        self.storage.save_item_event(&checklist_item_event)
            .or_raise(|| CrdtError::Recovered("crdt unable to add checklist item".into()))?;

        let success_text = "crdt unable to save stamp. addition of checklist item reverted";
        let failure_text = "crdt unable to save stamp. reversion of checklist item addition failed. crdt is in inconsistent state";
        let reversion_f = |crdt: &mut ChecklistCrdt<S, T>, id: &Uuid| crdt.storage.delete_item_event(id).map(|_| ());
        self.save_stamp_or_revert(stamp, reversion_f, id, success_text, failure_text)
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

        let crdt = ChecklistCrdt::new(file_store, transport);
    }
}
