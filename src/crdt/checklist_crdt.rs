use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::mem::discriminant;

use exn::{bail, Exn, Result, ResultExt};
use itc::{EventTree, IdTree, IntervalTreeClock, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::crdt::crdt_error::CrdtError;
use crate::persistence::model::checklist::{HeadEvent, ItemEvent};
use crate::persistence::{StorageError, ErrorKind};
use crate::persistence::storage::Store;
use crate::transport::transport::Transport;


#[macro_export]
macro_rules! transaction {
    ($this:expr, $stamp:expr, $body:block) => {{
        $this.storage
            .start_transaction()
            .or_raise(|| CrdtError::recovered("unable to start transaction"))?;

        let result = (|| -> Result<_, _> { $body })()?;

        let _ = $this.storage.save_stamp(&$stamp).map_err(|e| {
            $this.abort_transaction(e, "crdt unable to save stamp")
        })?;

        let _ = $this.storage.commit_transaction().map_err(|e| {
            $this.abort_transaction(e, "crdt unable commit transaction")
        })?;

        $this.itc_stamp = $stamp;

        Ok(result)
    }};
}


pub struct EventDelta {
    from_itc_event: EventTree,
    to_itc_event: EventTree,
    head_events: Vec<HeadEvent>,
    item_events: Vec<ItemEvent>,
}

impl EventDelta {
    pub fn new(
        from_itc_event: EventTree,
        to_itc_event: EventTree,
        head_events: Vec<HeadEvent>,
        item_events: Vec<ItemEvent>,
    ) -> Self {
        Self { from_itc_event, to_itc_event, head_events, item_events }
    }
}


#[derive(Debug, PartialEq)]
pub struct Peer {
    stamp: Stamp,
    head_events: Vec<HeadEvent>,
    item_events: Vec<ItemEvent>,
}

impl Peer {
    pub fn new(
        stamp: Stamp,
        head_events: Vec<HeadEvent>,
        item_events: Vec<ItemEvent>,
    ) -> Self {
        Self { stamp, head_events, item_events }
    }
}


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
            Err(_) => bail!(CrdtError::fatal("failed to load stamp")),
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

    fn save_checklist_head_event(&mut self, event: HeadEvent) -> Result<(), CrdtError> {
        self.storage.save_head_event(&event).map_err(|e|
            self.abort_transaction(e, "crdt unable to store head event")
        )
    }

    fn erase_checklist_head(
        &mut self,
        id: &Uuid,
    ) -> Result<HeadEvent, CrdtError> {
        let stamp = self.itc_stamp.event();

        transaction!(self, stamp, {
            self.storage.delete_head_event(&id).map_err(|e|
                self.abort_transaction(e, "crdt unable to erase head event")
            )
        })
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

        let _: Result<(), CrdtError> = transaction!(self, stamp, {
            self.save_checklist_head_event(event)
        });
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

        transaction!(self, stamp, { self.save_checklist_head_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_head_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_head_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_head_event(event) })
    }

    pub fn get_all_checklist_heads(&self) -> Result<Vec<HeadEvent>, CrdtError> {
        self.storage.load_all_head_events()
            .or_raise(|| CrdtError::recovered("crdt unable to load all head events"))
    }

    fn save_checklist_item_event(&mut self, event: ItemEvent) -> Result<(), CrdtError> {
        self.storage.save_item_event(&event).map_err(|e|
            self.abort_transaction(e, "crdt unable to store item event")
        )
    }

    fn erase_checklist_item(
        &mut self,
        id: &Uuid,
    ) -> Result<ItemEvent, CrdtError> {
        let stamp = self.itc_stamp.event();

        transaction!(self, stamp, {
            self.storage.delete_item_event(id).map_err(|e|
                self.abort_transaction(e, "crdt unable to erase item event")
            )
        })
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

        let _: Result<(), CrdtError> = transaction!(self, stamp, {
            self.save_checklist_item_event(event)
        });
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

        transaction!(self, stamp, { self.save_checklist_item_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_item_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_item_event(event) })
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

        transaction!(self, stamp, { self.save_checklist_item_event(event) })
    }

    pub fn get_all_checklist_items(&self) -> Result<Vec<ItemEvent>, CrdtError> {
        self.storage.load_all_item_events()
            .or_raise(|| CrdtError::recovered("crdt unable to load all item events"))
    }

    pub fn get_event_delta(&self, peer_event: EventTree) -> Result<EventDelta, CrdtError> {
        let mut head_events = self.get_all_checklist_heads()?;
        head_events.retain(|head| {
            match peer_event.partial_cmp(head.itc_event()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let mut item_events = self.get_all_checklist_items()?;
        item_events.retain(|item| {
            match peer_event.partial_cmp(item.itc_event()) {
                Some(ordering) => ordering == Ordering::Less,
                None => true,
            }
        });

        let own_event = self.itc_stamp.event_tree();

        Ok(EventDelta::new(peer_event, own_event, head_events, item_events))
    }

    pub fn apply_event_delta(&mut self, delta: EventDelta) -> Result<(), CrdtError> {
        // if self.itc_stamp.event_tree() != delta.from_itc_event {
        //     bail!(CrdtError::recovered(""));
        // }

        let peer_stamp = Stamp::new(IdTree::zero(), delta.to_itc_event.clone());
        let stamp = self.itc_stamp.join(&peer_stamp).event();

        transaction!(self, stamp, {
            for event in delta.head_events {
                self.save_checklist_head_event(event)?;
            }

            for event in delta.item_events {
                self.save_checklist_item_event(event)?;
            }

            Ok(())
        })
    }

    pub fn fork(&mut self) -> Result<Peer, CrdtError> {
        let (stamp, peer_stamp) = self.itc_stamp.fork();
        let head_events = self.get_all_checklist_heads()?;
        let item_events = self.get_all_checklist_items()?;

        let _: Result<(), CrdtError> = transaction!(self, stamp, { Ok(()) });
        Ok(Peer::new(peer_stamp, head_events, item_events))
    }

    fn find_trimable_head_events(&self) -> Result<(Vec<Uuid>, HashSet<Uuid>), CrdtError> {
        let events = self.get_all_checklist_heads()?;
        let grouped_events = events.iter()
            .fold(HashMap::new(), |mut m: HashMap<Uuid, Vec<&HeadEvent>>, e| {
                m.entry(e.head_id().clone()).or_default().push(e);
                m
            });

        let mut trimable_events = Vec::new();
        let mut deleted_events = Vec::new();
        let deletion_discriminant = discriminant(&HeadEvent::Deletion { id: Uuid::default(), head_id: Uuid::default(), itc_event: EventTree::zero() });
        for group in grouped_events.into_values() {
            let discriminant_bins = group.into_iter()
                .fold(HashMap::new(), |mut m: HashMap<_,Vec<&HeadEvent>>, e| {
                    m.entry(discriminant(e)).or_default().push(e);
                    m
                });

            let mut keep = Vec::new();
            let mut headstone = None;
            for mut bin in discriminant_bins.into_values() {
                if let Some(event) = bin.pop() {
                    if discriminant(event) == deletion_discriminant {
                        headstone = Some(event)
                    } else {
                        keep.push(event);
                    }
                    trimable_events.extend( bin);
                }
            }
            if let Some(headstone) = headstone {
                trimable_events.extend(keep);
                deleted_events.push(headstone);
            }
        }

        let trimable_events_ids = trimable_events.into_iter()
            .map(|event| event.id().clone())
            .collect::<Vec<Uuid>>();

        let deleted_events_ids = deleted_events.into_iter().map(|event| {
                match event {
                    HeadEvent::Deletion { head_id, .. } => head_id.clone(),
                    _ => unreachable!()
                }
            })
            .collect::<HashSet<Uuid>>();

        Ok((trimable_events_ids, deleted_events_ids))
    }

    fn find_trimable_item_events(&self, deleted_head_events_ids: HashSet<Uuid>) -> Result<Vec<Uuid>, CrdtError> {
        let events = self.get_all_checklist_items()?;
        let grouped_events = events.iter()
            .fold(HashMap::new(), |mut m: HashMap<Uuid, Vec<&ItemEvent>>, e| {
                m.entry(e.item_id().clone()).or_default().push(e);
                m
            });

        let mut trimable_events = Vec::new();
        let mut deleted_events = Vec::new();
        for group in grouped_events.into_values() {
            let discriminant_bins = group.into_iter()
                .fold(HashMap::new(), |mut m: HashMap<_,Vec<&ItemEvent>>, e| {
                    m.entry(discriminant(e)).or_default().push(e);
                    m
                });

            let mut keep = Vec::new();
            let mut headstone = None;
            let mut delete_all = false;
            for mut bin in discriminant_bins.into_values() {
                let event = match bin.pop() {
                    Some(e) => e,
                    None => continue,
                };
                match event {
                    ItemEvent::Creation { head_id, .. } => {
                        if deleted_head_events_ids.contains(&head_id) {
                            delete_all = true;
                        }
                        keep.push(event);
                    },
                    ItemEvent::Deletion { .. } => headstone = Some(event),
                    _ => keep.push(event),
                }
                trimable_events.extend( bin);
            }

            if delete_all {
                trimable_events.extend(keep);
                if let Some(headstone) = headstone {
                    trimable_events.push(headstone);
                }
            } else if let Some(headstone) = headstone {
                trimable_events.extend(keep);
                deleted_events.push(headstone);
            }
        }

        let trimable_events_ids = trimable_events.into_iter()
            .map(|event| event.id().clone())
            .collect::<Vec<Uuid>>();

        Ok(trimable_events_ids)
    }

    pub fn trim_events(&mut self) -> Result<(Vec<HeadEvent>, Vec<ItemEvent>), CrdtError> {
        let (trimable_head_events_ids, deleted_head_events_ids) = self.find_trimable_head_events()
            .or_raise(|| CrdtError::recoverable("failed to find all trimable head events"))?;
        let trimable_item_events_ids = self.find_trimable_item_events(deleted_head_events_ids)
            .or_raise(|| CrdtError::recoverable("failed to find all trimable item events"))?;

        let stamp = self.itc_stamp.event();

        transaction!(self, stamp, {
            let mut deleted_head_events = Vec::new();
            let mut deleted_item_events = Vec::new();
            for head_event_id in trimable_head_events_ids {
                let head_event = self.storage.delete_head_event(&head_event_id).map_err(|e|
                    self.abort_transaction(e, "crdt unable to store head event")
                )?;
                deleted_head_events.push(head_event);
            }

            for item_event_id in trimable_item_events_ids {
                let item_event = self.storage.delete_item_event(&item_event_id).map_err(|e|
                    self.abort_transaction(e, "crdt unable to store item event")
                )?;
                deleted_item_events.push(item_event);
            }

            Ok((deleted_head_events, deleted_item_events))
        })
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn init_test() {
        use crate::persistence::storage::FileStorage;

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
