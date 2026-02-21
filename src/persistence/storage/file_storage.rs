use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::str::{FromStr, Split};

use exn::{OptionExt, Result, ResultExt, bail};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadEvent, ItemEvent};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnMut(&mut FileStorage) -> Result<(), StorageError>>;

struct EventLogFile {
    file: File,
    event_positions: Vec<(u64, Uuid)>,
}


pub struct FileStorage {
    stamp_file: File,
    head_log_file: EventLogFile,
    item_log_file: EventLogFile,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl FileStorage {
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
            .or_raise(|| StorageError::backend_open(format!("failed to open stamp file at {stamp_path}")))?;

        let head_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(head_log_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open event log file at {head_log_path}")))?;

        let mut offset: u64 = 0;
        let head_positions: Vec<(u64, Uuid)> = FileStorage::load_all_head_events_with_length(&head_log_file)?
            .into_iter()
            .map(|head| {
                offset += head.0;
                (offset, head.1.id().clone())
            }).collect();

        let item_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(item_log_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open event log file at {item_log_path}")))?;

        let mut offset: u64 = 0;
        let item_positions: Vec<(u64, Uuid)> = FileStorage::load_all_item_events_with_length(&item_log_file)?
            .into_iter()
            .map(|item| {
                offset += item.0;
                (offset, item.1.id().clone())
            }).collect();

        let file_store = FileStorage {
            stamp_file: stamp_file,
            head_log_file: EventLogFile { file: head_log_file, event_positions: head_positions },
            item_log_file: EventLogFile { file: item_log_file, event_positions: item_positions },
            in_transaction: false,
            rollback_stack: Vec::new(),
        };

        Ok(file_store)
    }

    fn get_next_str<'a>(
        iter: &mut Split<'a, &str>,
        expected_value: &str,
    ) -> Result<&'a str, StorageError> {
        iter.next().ok_or_raise(|| StorageError::data_decode(
            format!("expected {}, found end of line", expected_value)
        ))
    }

    fn get_next_string(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<String, StorageError> {
        let first = FileStorage::get_next_str(iter, &format!("word 1 of {}", expected_value))?;

        let mut words = Vec::<&str>::new();
        let (raw_count, word) = first.split_once(":")
            .ok_or_raise(|| StorageError::data_decode(
                format!("unable to split '{}' for length value", first)
            ))?;
        words.push(word);

        let count = raw_count.parse::<u64>()
            .or_raise(|| StorageError::data_decode(
                format!("unable to parse '{}' as word count", raw_count)
            ))?;

        for word_number in 2..=count {
            let next_word = FileStorage::get_next_str(
                iter, &format!("word {} of {}", word_number, expected_value)
            )?;
            words.push(next_word)
        }

        Ok(words.join(" "))
    }

    fn parse_uuid(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<Uuid, StorageError> {
        FileStorage::get_next_str(iter, expected_value)?
            .parse::<Uuid>()
            .or_raise(|| StorageError::data_decode("failed to decode Uuid"))
    }

    fn parse_optional_uuid(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<Option<Uuid>, StorageError> {
        let s = FileStorage::get_next_str(iter, expected_value)?;

        if s.is_empty() {
            return Ok(None);
        }

        s.parse::<Uuid>()
            .map(Some)
            .or_raise(|| StorageError::data_decode("failed to decode Uuid"))
    }

    fn parse_itc_event(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<EventTree, StorageError> {
        FileStorage::get_next_str(iter, expected_value)?
            .parse::<EventTree>()
            .or_raise(|| StorageError::data_decode("failed to decode EventTree"))
    }

    fn parse_bool(iter: &mut Split<'_, &str>, expected_value: &str) -> Result<bool, StorageError> {
        FileStorage::get_next_str(iter, expected_value)?
            .parse::<bool>()
            .or_raise(|| StorageError::data_decode("failed to decode bool"))
    }

    fn parse_event_meta(iter: &mut Split<'_, &str>) -> Result<(Uuid, EventTree), StorageError> {
        let id = FileStorage::parse_uuid(iter, "id")?;
        let itc_event = FileStorage::parse_itc_event(iter, "itc event")?;

        Ok((id, itc_event))
    }

    fn serialize_head_creation(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::Creation { id, itc_event, template_id, name, description } = event {
            let description = description.clone().unwrap_or(String::new());
            Some(format!(
                "Creation {} {} {} {}:{} {}:{}\n",
                id,
                itc_event,
                template_id.map_or(String::new(), |id| id.to_string()),
                name.matches(" ").count()+1,
                name,
                description.matches(" ").count()+1,
                description,
            ))
        } else {
            None
        }
    }

    fn parse_head_creation(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;

        let template_id = FileStorage::parse_optional_uuid(iter, "template id")?;

        let name = FileStorage::get_next_string(iter, "name")?;

        let description = Some(FileStorage::get_next_string(iter, "description")?)
            .filter(|s| s.is_empty());

        Ok(HeadEvent::Creation { id, itc_event, template_id, name, description })
    }

    fn serialize_head_name_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::NameUpdate { id, itc_event, head_id, name } = event {
            Some(format!(
                "NameUpdate {} {} {} {}:{}\n",
                id,
                itc_event,
                head_id,
                name.matches(" ").count()+1,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_head_name_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let head_id = FileStorage::parse_uuid(iter, "head id")?;
        let name = FileStorage::get_next_string(iter, "name")?;

        Ok(HeadEvent::NameUpdate { id, itc_event, head_id, name })
    }

    fn serialize_head_description_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::DescriptionUpdate { id, itc_event, head_id, description } = event {
            let description = description.clone().unwrap_or(String::new());
            Some(format!(
                "DescriptionUpdate {} {} {} {}:{}\n",
                id,
                itc_event,
                head_id,
                description.matches(" ").count()+1,
                description,
            ))
        } else {
            None
        }
    }

    fn parse_head_description_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let head_id = FileStorage::parse_uuid(iter, "head id")?;
        let description = Some(FileStorage::get_next_string(iter, "description")?)
            .filter(|s| s.is_empty());

        Ok(HeadEvent::DescriptionUpdate { id, itc_event, head_id, description })
    }

    fn serialize_head_completed_update(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::CompletedUpdate { id, itc_event, head_id, completed } = event {
            Some(format!(
                "CompletedUpdate {} {} {} {}\n",
                id,
                itc_event,
                head_id,
                completed,
            ))
        } else {
            None
        }
    }

    fn parse_head_completed_update(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let head_id = FileStorage::parse_uuid(iter, "head id")?;
        let completed = FileStorage::parse_bool(iter, "completed")?;

        Ok(HeadEvent::CompletedUpdate { id, itc_event, head_id, completed })
    }

    fn serialize_head_deletion(&mut self, event: &HeadEvent) -> Option<String> {
        if let HeadEvent::Deletion { id, head_id, itc_event } = event {
            Some(format!(
                "Deletion {} {} {}\n",
                id,
                itc_event,
                head_id,
            ))
        } else {
            None
        }
    }

    fn parse_head_deletion(iter: &mut Split<'_, &str>) -> Result<HeadEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let head_id = FileStorage::parse_uuid(iter, "head id")?;

        Ok(HeadEvent::Deletion { id, itc_event, head_id })
    }

    fn load_head_event(line: &str) -> Result<(u64, HeadEvent), StorageError> {
        let mut parts = line.split(" ");
        let head = match parts.next() {
            Some("Creation") => FileStorage::parse_head_creation(&mut parts),
            Some("NameUpdate") => FileStorage::parse_head_name_update(&mut parts),
            Some("DescriptionUpdate") => FileStorage::parse_head_description_update(&mut parts),
            Some("CompletedUpdate") => FileStorage::parse_head_completed_update(&mut parts),
            Some("Deletion") => FileStorage::parse_head_deletion(&mut parts),
            Some(prefix) => bail!(StorageError::data_decode(format!("unexpected prefix '{}'", prefix))),
            None => bail!(StorageError::data_decode("expected prefix, found end of line")),
        }?;
        Ok((line.len() as u64 + 1, head))
    }

    fn load_all_head_events_with_length(file: &File) -> Result<Vec<(u64, HeadEvent)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStorage::load_head_event(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, HeadEvent)>, StorageError>>()
            .or_raise(|| StorageError::data_decode("unable to parse all lines"))
    }

    fn serialize_item_creation(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::Creation { id, itc_event, head_id, name, position } = event {
            Some(format!(
                "Creation {} {} {} {}:{} {}\n",
                id,
                itc_event,
                head_id,
                name.matches(" ").count()+1,
                name,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_creation(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;

        let head_id = FileStorage::parse_uuid(iter, "head id")?;

        let name = FileStorage::get_next_string(iter, "name")?;

        let position = FileStorage::get_next_str(iter, "position")?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ItemEvent::Creation { id, itc_event, head_id, name, position })
    }

    fn serialize_item_name_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::NameUpdate { id, itc_event, item_id, name } = event {
            Some(format!(
                "NameUpdate {} {} {} {}:{}\n",
                id,
                itc_event,
                item_id,
                name.matches(" ").count()+1,
                name,
            ))
        } else {
            None
        }
    }

    fn parse_item_name_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let item_id = FileStorage::parse_uuid(iter, "item id")?;

        let name = FileStorage::get_next_string(iter, "name")?;

        Ok(ItemEvent::NameUpdate { id, itc_event, item_id, name })
    }

    fn serialize_item_position_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::PositionUpdate { id, itc_event, item_id, position } = event {
            Some(format!(
                "PositionUpdate {} {} {} {}\n",
                id,
                itc_event,
                item_id,
                position,
            ))
        } else {
            None
        }
    }

    fn parse_item_position_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let item_id = FileStorage::parse_uuid(iter, "item id")?;

        let position = FileStorage::get_next_str(iter, "position")?;
        let position = FractionalIndex::from_hex_string(position);

        Ok(ItemEvent::PositionUpdate { id, itc_event, item_id, position })
    }

    fn serialize_item_checked_update(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::CheckedUpdate { id, itc_event, item_id, checked } = event {
            Some(format!(
                "CheckedUpdate {} {} {} {}\n",
                id,
                itc_event,
                item_id,
                checked,
            ))
        } else {
            None
        }
    }

    fn parse_item_checked_update(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let item_id = FileStorage::parse_uuid(iter, "item id")?;
        let checked = FileStorage::parse_bool(iter, "checked")?;

        Ok(ItemEvent::CheckedUpdate { id, itc_event, item_id, checked })
    }

    fn serialize_item_deletion(&mut self, event: &ItemEvent) -> Option<String> {
        if let ItemEvent::Deletion { id, itc_event, item_id } = event {
            Some(format!(
                "Deletion {} {} {}\n",
                id,
                itc_event,
                item_id,
            ))
        } else {
            None
        }
    }

    fn parse_item_deletion(iter: &mut Split<'_, &str>) -> Result<ItemEvent, StorageError> {
        let (id, itc_event) = FileStorage::parse_event_meta(iter)?;
        let item_id = FileStorage::parse_uuid(iter, "item id")?;

        Ok(ItemEvent::Deletion { id, itc_event, item_id })
    }

    fn load_item_event(line: &str) -> Result<(u64, ItemEvent), StorageError> {
        let mut parts = line.split(" ");
        let event = match parts.next() {
            Some("Creation") => FileStorage::parse_item_creation(&mut parts),
            Some("NameUpdate") => FileStorage::parse_item_name_update(&mut parts),
            Some("PositionUpdate") => FileStorage::parse_item_position_update(&mut parts),
            Some("CheckedUpdate") => FileStorage::parse_item_checked_update(&mut parts),
            Some("Deletion") => FileStorage::parse_item_deletion(&mut parts),
            Some(prefix) => bail!(StorageError::data_decode(format!("unexpected prefix '{}'", prefix))),
            None => bail!(StorageError::data_decode("expected prefix, found end of line")),
        }?;
        Ok((line.len() as u64 + 1, event))
    }

    fn load_all_item_events_with_length(file: &File) -> Result<Vec<(u64, ItemEvent)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => FileStorage::load_item_event(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, ItemEvent)>, StorageError>>()
            .or_raise(|| StorageError::data_decode("unable to parse all lines"))
    }
}


impl Store for FileStorage {
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
            if let Err(e) = rollback_function(self) {
                self.rollback_stack.clear();
                return Result::Err(e.raise(
                    StorageError::transaction_rollback_partial("failed to rollback all transaction steps")
                ));
            }
        }

        self.in_transaction = false;
        Ok(true)
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        if !self.in_transaction {
            return  Ok(false);
        }
        self.rollback_stack.clear();
        Ok(true)
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        if self.in_transaction {
            let file_size = self.stamp_file.metadata()
                .or_raise(|| StorageError::backend_specific("failed to read stamp file metadata"))?
                .len();

            if file_size == 0 {
                self.rollback_stack.push(Box::new(move |store: &mut FileStorage| {
                    store.stamp_file.set_len(0)
                        .or_raise(|| StorageError::backend_specific("failed to truncate stamp file"))
                }));
            } else {
                let current_stamp = self.load_stamp()?;
                self.rollback_stack.push(Box::new(move |store: &mut FileStorage| store.save_stamp(&current_stamp)));
            }
        }

        self.stamp_file.set_len(0)
            .or_raise(|| StorageError::backend_specific("failed to truncate stamp file"))?;

        self.stamp_file.seek(SeekFrom::Start(0))
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp event file"))?;

        self.stamp_file.write(stamp.to_string().as_bytes())
            .or_raise(|| StorageError::backend_write("failed to write stamp to file"))?;

        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        let file_size = self.stamp_file.metadata()
            .or_raise(|| StorageError::backend_specific("failed to read stamp file metadata"))?
            .len();
        if file_size == 0 {
            bail!(StorageError::stamp_none("stamp file does not contain a stamp"))
        }

        self.stamp_file.seek(SeekFrom::Start(0))
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp event file"))?;

        let mut stamp_buf = String::new();
        self.stamp_file.read_to_string(&mut stamp_buf)
            .or_raise(|| StorageError::backend_read("failed to read stamp file"))?;
        let stamp = Stamp::from_str(&stamp_buf)
            .or_raise(|| StorageError::stamp_invalid(format!("failed to parse stamp '{}'", &stamp_buf)))?;

        Ok(stamp)
    }

    fn save_head_event(&mut self, event: &HeadEvent) -> Result<(), StorageError> {
        let (line, id) = {
            use HeadEvent::*;
            match event {
                Creation { .. } => self.serialize_head_creation(event),
                NameUpdate { .. } => self.serialize_head_name_update(event),
                DescriptionUpdate { .. } => self.serialize_head_description_update(event),
                CompletedUpdate { .. } => self.serialize_head_completed_update(event),
                Deletion { .. } => self.serialize_head_deletion(event),
            }
        }
        .and_then(|s| Some((s, event.id().clone())))
        .ok_or_else(|| StorageError::data_encode(format!("unable to serialize head event with id {}", event.id())))?;

        let file = &mut self.head_log_file.file;
        let event_positions = &mut self.head_log_file.event_positions;

        let index = match event_positions.binary_search_by_key(&id, |i| i.1) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("head event with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let line_size = line.len() as u64;
        let start = index
            .checked_sub(1)
            .map(|i| event_positions[i].0)
            .unwrap_or(0);
        let end = start+line_size;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head event file"))?;

        if index == event_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head event '{id}' to file")))?;
            event_positions.push((end, id.clone()));
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from head event file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate head event file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head event '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head event file"))?;

            event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 += line_size);

            event_positions.insert(index, (end, id.clone()));
        }

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage|
                store.delete_head_event(&id).map(|_| ())
            ));
        }

        Ok(())
    }

    fn load_all_head_events(&self) -> Result<Vec<HeadEvent>, StorageError> {
        Ok(FileStorage::load_all_head_events_with_length(&self.head_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn delete_head_event(&mut self, id: &Uuid) -> Result<HeadEvent, StorageError> {
        let file = &mut self.head_log_file.file;
        let event_positions = &mut self.head_log_file.event_positions;

        let index = match event_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of head event with id '{id}'")
            )),
        };

        let start = index
            .checked_sub(1)
            .map(|i| event_positions[i].0)
            .unwrap_or(0);
        let end = event_positions.remove(index).0;
        let line_size = end-start;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head event file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read head event from file"))?;

        let head_slice = buffer.get(0 .. (line_size-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < event_positions.len() {
            let val = buffer.get(line_size as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let head_event = FileStorage::load_head_event(head_slice)
            .or_raise(|| StorageError::backend_read("failed to read head event from file"))?
            .1;

        file.set_len(start)
            .or_raise(|| StorageError::backend_specific("failed to truncate head event file"))?;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head event file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head event file"))?;
            event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= line_size);
        };

        if self.in_transaction {
            let cloned_head_event = head_event.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage| store.save_head_event(&cloned_head_event)));
        }

        Ok(head_event)
    }

    fn save_item_event(&mut self, event: &ItemEvent) -> Result<(), StorageError> {
        let (line, id) = {
            use ItemEvent::*;
            match event {
                Creation { .. } =>  self.serialize_item_creation(event),
                NameUpdate { .. } =>  self.serialize_item_name_update(event),
                PositionUpdate { .. } =>  self.serialize_item_position_update(event),
                CheckedUpdate { .. } =>  self.serialize_item_checked_update(event),
                Deletion { .. } =>  self.serialize_item_deletion(event),
            }
        }
        .and_then(|s| Some((s, event.id().clone())))
        .ok_or_else(|| StorageError::data_encode(format!("unable to serialize item event with id {}", event.id())))?;

        let file = &mut self.item_log_file.file;
        let event_positions = &mut self.item_log_file.event_positions;

        let index = match event_positions.binary_search_by_key(&id, |i| i.1) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("item event with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let line_size = line.len() as u64;
        let start = index
            .checked_sub(1)
            .map(|i| event_positions[i].0)
            .unwrap_or(0);
        let end = start+line_size;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item event file"))?;

        if index == event_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item event '{id}' to file")))?;
            event_positions.push((end, id.clone()));
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from item event file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate item event file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item event '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item event file"))?;

            event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 += line_size);

            event_positions.insert(index, (end, id.clone()));
        }

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage|
                store.delete_item_event(&id).map(|_| ())
            ));
        }

        Ok(())
    }

    fn load_all_item_events(&self) -> Result<Vec<ItemEvent>, StorageError> {
        Ok(FileStorage::load_all_item_events_with_length(&self.item_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn delete_item_event(&mut self, id: &Uuid) -> Result<ItemEvent, StorageError> {
        let file = &mut self.item_log_file.file;
        let event_positions = &mut self.item_log_file.event_positions;

        let index = match event_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of item event with id '{id}'")
            )),
        };

        let start = index
            .checked_sub(1)
            .map(|i| event_positions[i].0)
            .unwrap_or(0);
        let end = event_positions.remove(index).0;
        let line_size = end-start;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item event file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read item event from file"))?;

        let item_slice = buffer.get(0 .. (line_size-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < event_positions.len() {
            let val = buffer.get(line_size as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let item_event = FileStorage::load_item_event(item_slice)
            .or_raise(|| StorageError::backend_read("failed to read item event from file"))?
            .1;

        file.set_len(start)
            .or_raise(|| StorageError::backend_specific("failed to truncate item event file"))?;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item event file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item event file"))?;
            event_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= line_size);
        };

        if self.in_transaction {
            let cloned_item_event = item_event.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage| store.save_item_event(&cloned_item_event)));
        }

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
        let mut file_store = FileStorage::new(
            stamp_path,
            head_log_path,
            item_log_path,
        ).unwrap();

        let head = HeadEvent::Creation {
            id: uuid::Uuid::now_v7(),
            itc_event: itc::EventTree::zero(),
            template_id: Some(uuid::Uuid::new_v4()),
            name: "test test".into(),
            description: Some("this is a description".into())
        };

        file_store.save_head_event(&head).unwrap();
    }
}
