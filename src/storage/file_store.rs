use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::str::{FromStr, Split};

use exn::{OptionExt, Result, ResultExt, bail};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use super::model::checklist::head::HeadEvent;
use super::model::checklist::item::ItemEvent;
use super::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnMut(&mut FileStore) -> Result<(), StorageError>>;

struct EventLogFile {
    file: File,
    event_positions: Vec<(u64, Uuid)>,
}


pub struct FileStore {
    stamp_file: File,
    head_log_file: EventLogFile,
    item_log_file: EventLogFile,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl FileStore {
    pub fn new(
        stamp_path: &str,
        head_log_path: &str,
        item_log_path: &str,
    )  -> Result<Self, StorageError> {
        let stamp_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(stamp_path)
            .or_raise(|| StorageError(format!("failed to open stamp file at {stamp_path}")))?;

        let head_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(head_log_path)
            .or_raise(|| StorageError(format!("failed to open event log file at {head_log_path}")))?;

        let mut offset: u64 = 0;
        let head_positions: Vec<(u64, Uuid)> = FileStore::load_all_head_events_with_length(&head_log_file)?
            .into_iter()
            .map(|head| {
                let cur_offset = offset;
                offset += head.0+1;
                let uuid = head.1.id().clone();
                (cur_offset, uuid)
            }).collect();

        let item_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(item_log_path)
            .or_raise(|| StorageError(format!("failed to open event log file at {item_log_path}")))?;

        let mut offset: u64 = 0;
        let item_positions: Vec<(u64, Uuid)> = FileStore::load_all_item_events_with_length(&item_log_file)?
            .into_iter()
            .map(|item| {
                let cur_offset = offset;
                offset += item.0+1;
                let uuid = item.1.id().clone();
                (cur_offset, uuid)
            }).collect();

        let file_store = FileStore {
            stamp_file: stamp_file,
            head_log_file: EventLogFile { file: head_log_file, event_positions: head_positions },
            item_log_file: EventLogFile { file: item_log_file, event_positions: item_positions },
            in_transaction: false,
            rollback_stack: Vec::new(),
        };

        Ok(file_store)
    }

    fn parse_event_meta(iter: &mut Split<'_, &str>) -> Result<(Uuid, EventTree), StorageError> {
        let id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;

        let itc_event = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<EventTree>()
            .map_err(|_| StorageError("".into()))?;

        Ok((id, itc_event))
    }

    fn save_head_creation(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::Creation { id, itc_event, template_id, name, description } = event {
            Some(format!(
                "Creation {} {} {} {} {}\n",
                id,
                itc_event,
                template_id.unwrap_or(Uuid::nil()),
                name,
                description.clone().unwrap_or(String::new()),
            ))
        } else {
            None
        }
    }

    fn parse_head_creation(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let template_id = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<Uuid>()
            .map_err(|_| StorageError("".into()))?;
        let template_id = (!template_id.is_nil()).then_some(template_id);

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        let description = iter.next()
            .map(|s| s.to_string());

        Ok(HeadEvent::Creation { id, itc_event, template_id, name, description })
    }

    fn save_head_name_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::NameUpdate { id, itc_event, name } = event {
            Some(format!(
                "NameUpdate {} {} {}\n",
                id,
                itc_event,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_head_name_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(HeadEvent::NameUpdate { id, itc_event, name })
    }

    fn save_head_description_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::DescriptionUpdate { id, itc_event, description } = event {
            Some(format!(
                "DescriptionUpdate {} {} {}\n",
                id,
                itc_event,
                description,
            ))
        } else {
            None
        }
    }

    fn parse_head_description_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let description = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(HeadEvent::DescriptionUpdate { id, itc_event, description })
    }

    fn save_head_completed_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::CompletedUpdate { id, itc_event, completed } = event {
            Some(format!(
                "CompletedUpdate {} {} {}\n",
                id,
                itc_event,
                completed,
            ))
        } else {
            None
        }
    }

    fn parse_head_completed_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let completed = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<bool>()
            .map_err(|_| StorageError("".into()))?;

        Ok(HeadEvent::CompletedUpdate { id, itc_event, completed })
    }

    fn save_head_deletion(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::Deletion { id, itc_event } = event {
            Some(format!(
                "Deletion {} {}\n",
                id,
                itc_event,
            ))
        } else {
            None
        }
    }

    fn parse_head_deletion(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        Ok(HeadEvent::Deletion { id, itc_event })
    }

    fn load_head_event(line: &str) -> Result<(u64, HeadEvent), StorageError> {
        let mut parts = line.split(" ");
        let head = match parts.next() {
            Some("Creation") => FileStore::parse_head_creation(&mut parts),
            Some("NameUpdate") => FileStore::parse_head_name_update(&mut parts),
            Some("DescriptionUpdate") => FileStore::parse_head_description_update(&mut parts),
            Some("CompletedUpdate") => FileStore::parse_head_completed_update(&mut parts),
            Some("Deletion") => FileStore::parse_head_deletion(&mut parts),
            Some(_) => bail!(StorageError("".into())),
            None => bail!(StorageError("".into())),
        }?;
        Ok((line.len() as u64, head))
    }

    fn load_all_head_events_with_length(file: &File) -> Result<Vec<(u64, HeadEvent)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStore::load_head_event(&l),
                    Err(e) => Err(e).or_raise(|| StorageError("".into())),
                }
            })
            .collect::<Result<Vec<(u64, HeadEvent)>, StorageError>>()
            .or_raise(|| StorageError("".into()))
    }

    fn save_item_creation(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::Creation { id, itc_event, head_id, name, position } = event {
            Some(format!(
                "Creation {} {} {} {} {}\n",
                id,
                itc_event,
                head_id,
                name,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_creation(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
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

        Ok(ItemEvent::Creation { id, itc_event, head_id, name, position })
    }

    fn save_item_name_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::NameUpdate { id, itc_event, name } = event {
            Some(format!(
                "NameUpdate {} {} {}\n",
                id,
                itc_event,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_item_name_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let name = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .to_string();

        Ok(ItemEvent::NameUpdate { id, itc_event, name })
    }

    fn save_item_position_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::PositionUpdate { id, itc_event, position } = event {
            Some(format!(
                "PositionUpdate {} {} {}\n",
                id,
                itc_event,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_position_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let position = iter.next()
            .ok_or_else(|| StorageError("".into()))?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ItemEvent::PositionUpdate { id, itc_event, position })
    }

    fn save_item_checked_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::CheckedUpdate { id, itc_event, checked } = event {
            Some(format!(
                "CheckedUpdate {} {} {}\n",
                id,
                itc_event,
                checked,
            ))
        } else {
            None
        }
    }

    fn parse_item_checked_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        let checked = iter.next()
            .ok_or_else(|| StorageError("".into()))?
            .parse::<bool>()
            .map_err(|_| StorageError("".into()))?;

        Ok(ItemEvent::CheckedUpdate { id, itc_event, checked })
    }

    fn save_item_deletion(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::Deletion { id, itc_event } = event {
            Some(format!(
                "Deletion {} {}\n",
                id,
                itc_event,
            ))
        } else {
            None
        }
    }

    fn parse_item_deletion(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStore::parse_event_meta(iter)
            .or_raise(|| StorageError("".into()))?;

        Ok(ItemEvent::Deletion { id, itc_event })
    }

    fn load_item_event(line: &str) -> Result<(u64, ItemEvent), StorageError> {
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
        Ok((line.len() as u64, event))
    }

    fn load_all_item_events_with_length(file: &File) -> Result<Vec<(u64, ItemEvent)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStore::load_item_event(&l),
                    Err(e) => Err(e).or_raise(|| StorageError("".into())),
                }
            })
            .collect::<Result<Vec<(u64, ItemEvent)>, StorageError>>()
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

    fn save_head_event(&mut self, event: &HeadEvent) -> Result<(), StorageError> {
        use HeadEvent::*;
        let (line, id) = match event {
            Creation { id, .. } => self.save_head_creation(event).and_then(|s| Some((s, id.clone()))),
            NameUpdate { id, .. } => self.save_head_name_update(event).and_then(|s| Some((s, id.clone()))),
            DescriptionUpdate { id, .. } => self.save_head_description_update(event).and_then(|s| Some((s, id.clone()))),
            CompletedUpdate { id, .. } => self.save_head_completed_update(event).and_then(|s| Some((s, id.clone()))),
            Deletion { id, .. } => self.save_head_deletion(event).and_then(|s| Some((s, id.clone()))),
        }.ok_or_else(|| StorageError("".into()))?;

        self.head_log_file.file.write(line.as_bytes())
            .or_raise(|| StorageError(format!("failed to write line to file")))?;

        if self.in_transaction {
            let cloned_id = id.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStore|
                store.delete_head_event(&cloned_id).map(|_| ())
            ));
        }

        self.head_log_file.event_positions.push((line.len() as u64, id));
        Ok(())
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        Ok(FileStore::load_all_head_events_with_length(&self.head_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn delete_head_event(&mut self, id: &uuid::Uuid) -> Result<HeadEvent, StorageError> {
        let index = match self.head_log_file.event_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError("".into())),
        };

        let (offset, _) = self.head_log_file.event_positions.remove(index);

        self.head_log_file.file.seek(SeekFrom::Start(offset))
            .or_raise(|| StorageError("".into()))?;

        let mut buffer = String::new();
        self.head_log_file.file.read_to_string(&mut buffer)
            .or_raise(|| StorageError("".into()))?;

        let (line, remainder) = if index < self.head_log_file.event_positions.len() {
            let next_offset = self.head_log_file.event_positions[index].0;
            let first_line_size = (next_offset-offset) as usize;
            let line_slice = buffer.get(0 .. first_line_size-1)
                .ok_or_raise(|| StorageError("".into()))?;
            let remainder_slice = buffer.get(first_line_size .. buffer.len()-1)
                .ok_or_raise(|| StorageError("".into()))?;

            (line_slice, Some((remainder_slice, first_line_size as u64)))
        } else {
            let line_slice = buffer.get(0 .. buffer.len()-1)
                .ok_or_raise(|| StorageError("".into()))?;
            (line_slice, None)
        };
        
        let head_event = FileStore::load_head_event(line)
            .or_raise(|| StorageError("".into()))?
            .1;

        if self.in_transaction {
            let cloned_head_event = head_event.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStore| store.save_head_event(&cloned_head_event)));
        }

        self.head_log_file.file.set_len(offset)
            .or_raise(|| StorageError(format!("")))?;

        if let Some((remainder, diff)) = remainder {
            self.head_log_file.file.write(remainder.as_bytes())
                .or_raise(|| StorageError(format!("")))?;
            self.head_log_file.event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= diff);
        };

        Ok(head_event)
    }

    fn save_item_event(&mut self, event: &ItemEvent) -> Result<(), StorageError> {
        use ItemEvent::*;
        let (line, id) = match event {
            Creation { id, .. } =>  self.save_item_creation(event).and_then(|s| Some((s, id.clone()))),
            NameUpdate { id, .. } =>  self.save_item_name_update(event).and_then(|s| Some((s, id.clone()))),
            PositionUpdate { id, .. } =>  self.save_item_position_update(event).and_then(|s| Some((s, id.clone()))),
            CheckedUpdate { id, .. } =>  self.save_item_checked_update(event).and_then(|s| Some((s, id.clone()))),
            Deletion { id, .. } =>  self.save_item_deletion(event).and_then(|s| Some((s, id.clone()))),
        }.ok_or_else(|| StorageError("".into()))?;

        self.item_log_file.file.write(line.as_bytes())
            .or_raise(|| StorageError(format!("failed to write line to file")))?;

        if self.in_transaction {
            let cloned_id = id.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStore|
                store.delete_item_event(&cloned_id).map(|_| ())
            ));
        }

        self.item_log_file.event_positions.push((line.len() as u64, id));

        Ok(())
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        Ok(FileStore::load_all_item_events_with_length(&self.item_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn delete_item_event(&mut self, id: &Uuid) -> Result<ItemEvent, StorageError> {
        let index = match self.item_log_file.event_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError("".into())),
        };

        let (offset, _) = self.item_log_file.event_positions.remove(index);

        self.item_log_file.file.seek(SeekFrom::Start(offset))
            .or_raise(|| StorageError("".into()))?;

        let mut buffer = String::new();
        self.item_log_file.file.read_to_string(&mut buffer)
            .or_raise(|| StorageError("".into()))?;

        let (line, remainder) = if index < self.item_log_file.event_positions.len() {
            let next_offset = self.item_log_file.event_positions[index].0;
            let first_line_size = (next_offset-offset) as usize;
            let line_slice = buffer.get(0 .. first_line_size-1)
                .ok_or_raise(|| StorageError("".into()))?;
            let remainder_slice = buffer.get(first_line_size .. buffer.len()-1)
                .ok_or_raise(|| StorageError("".into()))?;

            (line_slice, Some((remainder_slice, first_line_size as u64)))
        } else {
            let line_slice = buffer.get(0 .. buffer.len()-1)
                .ok_or_raise(|| StorageError("".into()))?;
            (line_slice, None)
        };
        
        let item_event = FileStore::load_item_event(line)
            .or_raise(|| StorageError("".into()))?
            .1;

        if self.in_transaction {
            let cloned_item_event = item_event.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStore| store.save_item_event(&cloned_item_event)));
        }

        self.item_log_file.file.set_len(offset)
            .or_raise(|| StorageError(format!("")))?;

        if let Some((remainder, diff)) = remainder {
            self.item_log_file.file.write(remainder.as_bytes())
                .or_raise(|| StorageError(format!("")))?;
            self.item_log_file.event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= diff);
        };

        Ok(item_event)
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_to_file() {
        let stamp_path = "./stamp.txt";
        let head_log_path = "./head_log.txt";
        let item_log_path = "./item_log.txt";
        let mut file_store = FileStore::new(
            stamp_path,
            head_log_path,
            item_log_path,
        ).unwrap();

        let head = HeadEvent::Creation {
            id: uuid::Uuid::new_v4(),
            itc_event: itc::EventTree::zero(),
            template_id: Some(uuid::Uuid::new_v4()),
            name: "test".into(),
            description: Some("this is a descr".into())
        };

        file_store.save_head_event(&head).unwrap();
    }
}
