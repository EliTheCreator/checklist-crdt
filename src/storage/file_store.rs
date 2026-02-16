use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::str::{FromStr, Split};

use exn::{bail, Result, ResultExt};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::storage::model::{HeadEventMeta, ItemEventMeta};

use super::model::{ChecklistHeadEvent, ChecklistItemEvent};
use super::storage_error::StorageError;
use super::store::Store;

type RollbackFunction = Box<dyn FnMut(&mut FileStore) -> Result<(), StorageError>>;

struct EventLogFile {
    file: File,
    event_positions: Option<Vec<(usize, Uuid)>>
}


pub struct FileStore {
    stamp_file: File,
    event_log_file: File,
    newlines: Vec<usize>,
    in_transaction: bool,
    rollback_stack: Vec<Box<dyn FnMut(&mut FileStore) -> Result<(), StorageError>>>,
}

impl FileStore {
    pub fn new(stamp_path: &str, event_log_path: &str)  -> Result<Self, StorageError> {
        let stamp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(stamp_path)
            .or_raise(|| StorageError(format!("failed to open stamp file at {stamp_path}")))?;

        let mut event_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(event_log_path)
            .or_raise(|| StorageError(format!("failed to open event log file at {event_log_path}")))?;

        let mut contents = String::new();
        event_log_file.read_to_string(&mut contents)
            .or_raise(|| StorageError("failed to read event log file".into()))?;

        let newlines = contents.match_indices('\n')
            .map(|pair| pair.0)
            .collect();

        let file_store = FileStore {
            stamp_file: stamp_file,
            event_log_file: event_log_file,
            newlines: newlines,
            in_transaction: false,
            rollback_stack: Vec::new(),
        };

        Ok(file_store)
    }

    fn parse_head_event_meta(iter: &mut Split<'_, &str>) -> Result<HeadEventMeta, StorageError> {
        let id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let head_id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let itc_event = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<EventTree>()
            .map_err(|_| StorageError("".into()))?;

        Ok(HeadEventMeta { id, head_id, itc_event })
    }

    fn save_head_creation(&mut self, event: &ChecklistHeadEvent) -> Option<String> {
        if let ChecklistHeadEvent::Creation { meta, template_id, name, description } = event {
            Some(format!(
                "Creation {} {} {} {} {} {}",
                meta.id,
                meta.head_id,
                meta.itc_event,
                template_id.unwrap_or(Uuid::nil()),
                name,
                description.clone().unwrap_or(String::new()),
            ))
        } else {
            None
        }
    }

    fn parse_head_creation(iter: &mut Split<'_, &str>) -> Result<ChecklistHeadEvent, StorageError> {
        let meta = FileStore::parse_head_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let template_id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;
        let template_id = if template_id.is_nil() {
            None
        } else {
            Some(template_id)
        };

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        let description = iter.next()
            .map(|s| s.to_string());

        Ok(ChecklistHeadEvent::Creation { meta, template_id, name, description })
    }

    fn save_head_name_update(&mut self, event: &ChecklistHeadEvent) -> Option<String> {
        if let ChecklistHeadEvent::NameUpdate { meta, name } = event {
            Some(format!(
                "NameUpdate {} {} {} {}",
                meta.id,
                meta.head_id,
                meta.itc_event,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_head_name_update(iter: &mut Split<'_, &str>) -> Result<ChecklistHeadEvent, StorageError> {
        let meta = FileStore::parse_head_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(ChecklistHeadEvent::NameUpdate { meta, name })
    }

    fn save_head_description_update(&mut self, event: &ChecklistHeadEvent) -> Option<String> {
        if let ChecklistHeadEvent::DescriptionUpdate { meta, description } = event {
            Some(format!(
                "DescriptionUpdate {} {} {} {}",
                meta.id,
                meta.head_id,
                meta.itc_event,
                description,
            ))
        } else {
            None
        }
    }

    fn parse_head_description_update(iter: &mut Split<'_, &str>) -> Result<ChecklistHeadEvent, StorageError> {
        let meta = FileStore::parse_head_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let description = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(ChecklistHeadEvent::DescriptionUpdate { meta, description })
    }

    fn save_head_completed_update(&mut self, event: &ChecklistHeadEvent) -> Option<String> {
        if let ChecklistHeadEvent::CompletedUpdate { meta, completed } = event {
            Some(format!(
                "CompletedUpdate {} {} {} {}",
                meta.id,
                meta.head_id,
                meta.itc_event,
                completed,
            ))
        } else {
            None
        }
    }

    fn parse_head_completed_update(iter: &mut Split<'_, &str>) -> Result<ChecklistHeadEvent, StorageError> {
        let meta = FileStore::parse_head_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let completed = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<bool>()
            .map_err(|_| StorageError("".into()))?;

        Ok(ChecklistHeadEvent::CompletedUpdate { meta, completed })
    }

    fn save_head_deletion(&mut self, event: &ChecklistHeadEvent) -> Option<String> {
        if let ChecklistHeadEvent::Deletion { meta } = event {
            Some(format!(
                "Deletion {} {} {}",
                meta.id,
                meta.head_id,
                meta.itc_event,
            ))
        } else {
            None
        }
    }

    fn parse_head_deletion(iter: &mut Split<'_, &str>) -> Result<ChecklistHeadEvent, StorageError> {
        let meta = FileStore::parse_head_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        Ok(ChecklistHeadEvent::Deletion { meta })
    }

    fn load_head_event(line: &str) -> Result<ChecklistHeadEvent, StorageError> {
        let mut parts = line.split(" ");
        match parts.next() {
            Some("Creation") => FileStore::parse_head_creation(&mut parts),
            Some("NameUpdate") => FileStore::parse_head_name_update(&mut parts),
            Some("DescriptionUpdate") => FileStore::parse_head_description_update(&mut parts),
            Some("CompletedUpdate") => FileStore::parse_head_completed_update(&mut parts),
            Some("Deletion") => FileStore::parse_head_deletion(&mut parts),
            Some(_) => bail!(StorageError("".into())),
            None => bail!(StorageError("".into())),
        }
    }

    fn parse_item_event_meta(iter: &mut Split<'_, &str>) -> Result<ItemEventMeta, StorageError> {
        let id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let item_id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let itc_event = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<EventTree>()
            .map_err(|_| StorageError("".into()))?;

        Ok(ItemEventMeta { id, item_id, itc_event })
    }

    fn save_item_creation(&mut self, event: &ChecklistItemEvent) -> Option<String> {
        if let ChecklistItemEvent::Creation { meta, head_id, name, position } = event {
            Some(format!(
                "Creation {} {} {} {} {} {}",
                meta.id,
                meta.item_id,
                meta.itc_event,
                head_id,
                name,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_creation(iter: &mut Split<'_, &str>) -> Result<ChecklistItemEvent, StorageError> {
        let meta = FileStore::parse_item_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let head_id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        let position = iter.next()
            .ok_or_else(|| StorageError("".into()))?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ChecklistItemEvent::Creation { meta, head_id, name, position })
    }

    fn save_item_name_update(&mut self, event: &ChecklistItemEvent) -> Option<String> {
        if let ChecklistItemEvent::NameUpdate { meta, name } = event {
            Some(format!(
                "NameUpdate {} {} {} {}",
                meta.id,
                meta.item_id,
                meta.itc_event,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_item_name_update(iter: &mut Split<'_, &str>) -> Result<ChecklistItemEvent, StorageError> {
        let meta = FileStore::parse_item_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(ChecklistItemEvent::NameUpdate { meta, name })
    }

    fn save_item_position_update(&mut self, event: &ChecklistItemEvent) -> Option<String> {
        if let ChecklistItemEvent::PositionUpdate { meta, position } = event {
            Some(format!(
                "PositionUpdate {} {} {} {}",
                meta.id,
                meta.item_id,
                meta.itc_event,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_position_update(iter: &mut Split<'_, &str>) -> Result<ChecklistItemEvent, StorageError> {
        let meta = FileStore::parse_item_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let position = iter.next()
            .ok_or_else(|| StorageError("".into()))?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ChecklistItemEvent::PositionUpdate { meta, position })
    }

    fn save_item_checked_update(&mut self, event: &ChecklistItemEvent) -> Option<String> {
        if let ChecklistItemEvent::CheckedUpdate { meta, checked } = event {
            Some(format!(
                "CheckedUpdate {} {} {} {}",
                meta.id,
                meta.item_id,
                meta.itc_event,
                checked,
            ))
        } else {
            None
        }
    }

    fn parse_item_checked_update(iter: &mut Split<'_, &str>) -> Result<ChecklistItemEvent, StorageError> {
        let meta = FileStore::parse_item_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let checked = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<bool>()
            .map_err(|_| StorageError("".into()))?;

        Ok(ChecklistItemEvent::CheckedUpdate { meta, checked })
    }

    fn save_item_deletion(&mut self, event: &ChecklistItemEvent) -> Option<String> {
        if let ChecklistItemEvent::Deletion { meta } = event {
            Some(format!(
                "Deletion {} {} {}",
                meta.id,
                meta.item_id,
                meta.itc_event,
            ))
        } else {
            None
        }
    }

    fn parse_item_deletion(iter: &mut Split<'_, &str>) -> Result<ChecklistItemEvent, StorageError> {
        let meta = FileStore::parse_item_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        Ok(ChecklistItemEvent::Deletion { meta })
    }

    fn load_item_event(line: &str) -> Result<(usize, ChecklistItemEvent), StorageError> {
        let mut parts = line.split(" ");
        let event = match parts.next() {
            Some("Creation") => FileStore::parse_item_creation(&mut parts),
            Some("NameUpdate") => FileStore::parse_item_name_update(&mut parts),
            Some("PositionUpdate") => FileStore::parse_item_position_update(&mut parts),
            Some("CheckedUpdate") => FileStore::parse_item_checked_update(&mut parts),
            Some("Deletion") => FileStore::parse_item_deletion(&mut parts),
            Some(_) => bail!(StorageError("".into())),
            None => bail!(StorageError("".into())),
        }?;
        Ok((line.len(), event))
    }

    fn load_all_item_events_with_length(&self) -> Result<Vec<(usize, ChecklistItemEvent)>, StorageError> {
        BufReader::new(&self.event_log_file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStore::load_item_event(&l),
                    Err(e) => Err(e).or_raise(|| StorageError("".into())),
                }
            })
            .collect::<Result<Vec<(usize, ChecklistItemEvent)>, StorageError>>()
            .or_raise(|| StorageError("".into()))
    }
}

impl Store for FileStore {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        let return_value = !self.in_transaction;
        self.in_transaction = true;
        Ok(return_value)
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }

        while let Some(mut rollback_function) = self.rollback_stack.pop() {
            rollback_function(self)
                .or_raise(|| StorageError("".into()))?
        }

        self.in_transaction = false;
        Ok(true)
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }
        Ok(true)
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        if self.in_transaction {
            let current_stamp = self.load_stamp()?;
            self.rollback_stack.push(Box::new(move |store: &mut FileStore| store.save_stamp(&current_stamp)));
        }

        self.stamp_file.set_len(0)
            .or_raise(|| StorageError("failed to truncate stamp file".into()))?;

        self.stamp_file.write(stamp.to_string().as_bytes())
            .or_raise(|| StorageError("failed to write stamp to file".into()))?;

        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        let mut stump_buf = String::new();
        self.stamp_file.read_to_string(&mut stump_buf)
            .or_raise(|| StorageError("failed to read stamp file".into()))?;
        let stamp = Stamp::from_str(&stump_buf)
            .or_raise(|| StorageError("failed to decode stamp".into()))?;

        Ok(stamp)
    }

    fn save_head_event(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError> {
        use ChecklistHeadEvent::*;
        let line = match event {
            Creation { meta, template_id, name, description }  => self.save_head_creation(event),
            NameUpdate { meta, name } => self.save_head_name_update(event),
            DescriptionUpdate { meta, description } => self.save_head_description_update(event),
            CompletedUpdate { meta, completed } => self.save_head_completed_update(event),
            Deletion { meta } => self.save_head_deletion(event),
        }.ok_or_else(|| StorageError("".into()))?;

        self.event_log_file.write(line.as_bytes())
            .or_raise(|| StorageError(format!("failed to write line to file")))?;
        Ok(())
    }

    fn load_all_head_events(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError> {
        BufReader::new(&self.event_log_file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStore::load_head_event(&l),
                    Err(e) => Err(e).or_raise(|| StorageError("".into())),
                }
            })
            .collect::<Result<Vec<ChecklistHeadEvent>, StorageError>>()
            .or_raise(|| StorageError("".into()))
    }

    fn delete_head_event(&mut self, head_id: &uuid::Uuid) -> Result<bool, StorageError> {
        todo!()
    }

    fn save_item_event(&mut self, event: &ChecklistItemEvent) -> Result<(), StorageError> {
        use ChecklistItemEvent::*;
        let line = match event {
            Creation {meta, head_id, name, position }  => self.save_item_creation(event),
            NameUpdate { meta, name } => self.save_item_name_update(event),
            PositionUpdate { meta, position } => self.save_item_position_update(event),
            CheckedUpdate { meta, checked } => self.save_item_checked_update(event),
            Deletion { meta } => self.save_item_deletion(event),
        }.ok_or_else(|| StorageError("".into()))?;

        self.event_log_file.write(line.as_bytes())
            .or_raise(|| StorageError(format!("failed to write line to file")))?;
        Ok(())
    }

    fn load_all_item_events(&self) -> Result<Vec<ChecklistItemEvent>, StorageError> {
        BufReader::new(&self.event_log_file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStore::load_item_event(&l),
                    Err(e) => Err(e).or_raise(|| StorageError("".into())),
                }
            })
            .collect::<Result<Vec<ChecklistItemEvent>, StorageError>>()
            .or_raise(|| StorageError("".into()))
    }

    fn delete_item_event(&mut self, item_id: &uuid::Uuid) -> Result<bool, StorageError> {
        todo!()
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_to_file() {
        let stamp_path = "./stamp.txt";
        let event_log_path = "./event_log.txt";
        let mut file_store = FileStore::new(stamp_path, event_log_path).unwrap();


        let meta = crate::storage::model::HeadEventMeta {
            id: uuid::Uuid::new_v4(),
            head_id: uuid::Uuid::new_v4(),
            itc_event: itc::EventTree::zero()
        };
        let head = ChecklistHeadEvent::Creation {
            meta: meta,
            template_id: Some(uuid::Uuid::new_v4()),
            name: "test".into(),
            description: Some("this is a descr".into())
        };

        file_store.save_head_event(&head).unwrap();
    }
}