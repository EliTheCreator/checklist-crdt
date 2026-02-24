use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::str::FromStr;

use exn::{OptionExt, Result, ResultExt, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


type RollbackFunction = Box<dyn FnOnce(&mut FileStorage) -> Result<(), StorageError>>;

struct OperationLogFile {
    file: File,
    operation_positions: Vec<(u64, Uuid)>,
}


pub struct FileStorage {
    stamp_file: File,
    head_log_file: OperationLogFile,
    item_log_file: OperationLogFile,
    in_transaction: bool,
    rollback_stack: Vec<RollbackFunction>,
}

impl FileStorage {
    fn open_file(path: &str)  -> std::result::Result<File, std::io::Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
    }

    pub fn new(
        stamp_path: &str,
        head_log_path: &str,
        item_log_path: &str,
    )  -> Result<Self, StorageError> {
        let stamp_file = Self::open_file(stamp_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open stamp file at {stamp_path}")))?;

        let head_log_file = Self::open_file(head_log_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open operation log file at {head_log_path}")))?;

        let mut offset: u64 = 0;
        let head_positions = Self::load_all_head_operations_with_length(&head_log_file)?
            .into_iter()
            .map(|head| {
                offset += head.0;
                (offset, head.1.id().clone())
            }).collect::<Vec<(u64, Uuid)>>();

        let item_log_file = Self::open_file(item_log_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open operation log file at {item_log_path}")))?;

        let mut offset: u64 = 0;
        let item_positions  = Self::load_all_item_operations_with_length(&item_log_file)?
            .into_iter()
            .map(|item| {
                offset += item.0;
                (offset, item.1.id().clone())
            }).collect::<Vec<(u64, Uuid)>>();

        let file_store = FileStorage {
            stamp_file: stamp_file,
            head_log_file: OperationLogFile { file: head_log_file, operation_positions: head_positions },
            item_log_file: OperationLogFile { file: item_log_file, operation_positions: item_positions },
            in_transaction: false,
            rollback_stack: Vec::new(),
        };

        Ok(file_store)
    }

    fn load_head_operation(line: &str) -> Result<(u64, HeadOperation), StorageError> {
        let length = line.len() + 1;
        let head = HeadOperation::try_from(line)
            .or_raise(|| StorageError::data_decode("unable to deserialize line"))?;
    
        Ok((length as u64, head))
    }

    fn load_all_head_operations_with_length(file: &File) -> Result<Vec<(u64, HeadOperation)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => Self::load_head_operation(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, HeadOperation)>, StorageError>>()
            .or_raise(|| StorageError::data_decode("unable to parse all lines"))
    }

    fn load_item_operation(line: &str) -> Result<(u64, ItemOperation), StorageError> {
        let length = line.len() + 1;
        let head = ItemOperation::try_from(line)
            .or_raise(|| StorageError::data_decode("unable to deserialize line"))?;
    
        Ok((length as u64, head))
    }

    fn load_all_item_operations_with_length(file: &File) -> Result<Vec<(u64, ItemOperation)>, StorageError> {
        BufReader::new(file)
            .lines()
            .map(|line| {
                match line {
                    Ok(l) => Self::load_item_operation(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, ItemOperation)>, StorageError>>()
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

        while let Some(rollback_function) = self.rollback_stack.pop() {
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
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp operation file"))?;

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
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp operation file"))?;

        let mut stamp_buf = String::new();
        self.stamp_file.read_to_string(&mut stamp_buf)
            .or_raise(|| StorageError::backend_read("failed to read stamp file"))?;
        let stamp = Stamp::from_str(&stamp_buf)
            .or_raise(|| StorageError::stamp_invalid(format!("failed to parse stamp '{}'", &stamp_buf)))?;

        Ok(stamp)
    }

    fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError> {
        let id = operation.id().clone();
        let line: String = operation.into();

        let file = &mut self.head_log_file.file;
        let operation_positions = &mut self.head_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(&id, |i| i.1) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("head operation with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let line_size = line.len() as u64;
        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].0)
            .unwrap_or(0);
        let end = start+line_size;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        if index == operation_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head operation '{id}' to file")))?;
            operation_positions.push((end, id.clone()));
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from head operation file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate head operation file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head operation '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head operation file"))?;

            operation_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 += line_size);

            operation_positions.insert(index, (end, id.clone()));
        }

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage|
                store.erase_head_operation(&id).map(|_| ())
            ));
        }

        Ok(())
    }

    fn load_all_head_operations(&self) -> Result<Vec<HeadOperation>, StorageError> {
        Ok(Self::load_all_head_operations_with_length(&self.head_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError> {
        let file = &mut self.head_log_file.file;
        let operation_positions = &mut self.head_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of head operation with id '{id}'")
            )),
        };

        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].0)
            .unwrap_or(0);
        let end = operation_positions.remove(index).0;
        let line_size = end-start;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read head operation from file"))?;

        let head_slice = buffer.get(0 .. (line_size-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < operation_positions.len() {
            let val = buffer.get(line_size as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let head_operation = Self::load_head_operation(head_slice)
            .or_raise(|| StorageError::backend_read("failed to read head operation from file"))?
            .1;

        file.set_len(start)
            .or_raise(|| StorageError::backend_specific("failed to truncate head operation file"))?;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head operation file"))?;
            operation_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= line_size);
        };

        if self.in_transaction {
            let cloned_head_operation = head_operation.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage| store.save_head_operation(cloned_head_operation)));
        }

        Ok(head_operation)
    }

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError> {
        let id = operation.id().clone();
        let line: String = operation.into();

        let file = &mut self.item_log_file.file;
        let operation_positions = &mut self.item_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(&id, |i| i.1) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("item operation with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let line_size = line.len() as u64;
        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].0)
            .unwrap_or(0);
        let end = start+line_size;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        if index == operation_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item operation '{id}' to file")))?;
            operation_positions.push((end, id.clone()));
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from item operation file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate item operation file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item operation '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item operation file"))?;

            operation_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 += line_size);

            operation_positions.insert(index, (end, id.clone()));
        }

        if self.in_transaction {
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage|
                store.erase_item_operation(&id).map(|_| ())
            ));
        }

        Ok(())
    }

    fn load_all_item_operations(&self) -> Result<Vec<ItemOperation>, StorageError> {
        Ok(Self::load_all_item_operations_with_length(&self.item_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError> {
        let file = &mut self.item_log_file.file;
        let operation_positions = &mut self.item_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(id, |i| i.1) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of item operation with id '{id}'")
            )),
        };

        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].0)
            .unwrap_or(0);
        let end = operation_positions.remove(index).0;
        let line_size = end-start;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read item operation from file"))?;

        let item_slice = buffer.get(0 .. (line_size-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < operation_positions.len() {
            let val = buffer.get(line_size as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let item_operation = Self::load_item_operation(item_slice)
            .or_raise(|| StorageError::backend_read("failed to read item operation from file"))?
            .1;

        file.set_len(start)
            .or_raise(|| StorageError::backend_specific("failed to truncate item operation file"))?;

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item operation file"))?;
            operation_positions.iter_mut()
                .skip(index)
                .for_each(|tuple| tuple.0 -= line_size);
        };

        if self.in_transaction {
            let cloned_item_operation = item_operation.clone();
            self.rollback_stack.push(Box::new(move |store: &mut FileStorage| store.save_item_operation(cloned_item_operation)));
        }

        Ok(item_operation)
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

        let head = HeadOperation::Creation {
            id: uuid::Uuid::now_v7(),
            itc_event: itc::EventTree::zero(),
            template_id: Some(uuid::Uuid::new_v4()),
            name: "test test".into(),
            description: Some("this is a description".into())
        };

        file_store.save_head_operation(head).unwrap();
    }
}
