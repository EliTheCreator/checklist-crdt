use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::str::FromStr;

use exn::{OptionExt, Result, ResultExt, bail};
use itc::Stamp;
use uuid::Uuid;

use crate::persistence::model::checklist::{head, item};
use crate::persistence::storage_error::StorageError;
use super::store::Store;


struct OperationMetadata {
    offset: u64,
    length: u64,
    id: Uuid,
    associated_id: Uuid,
}

impl OperationMetadata {
    fn new (
        offset: u64,
        length: u64,
        id: Uuid,
        associated_id: Uuid,
    ) -> Self {
        Self { offset, length, id, associated_id }
    }
}


struct OperationLogFile {
    file: File,
    operation_positions: Vec<OperationMetadata>,
}

impl OperationLogFile {
    fn new(
        file: File,
        operation_positions: Vec<OperationMetadata>,
    ) -> Self {
        Self { file, operation_positions }
    }
}


enum RollbackFunction {
    SaveHeadOperation(head::Operation),
    EraseHeadOperation(Uuid),
    SaveItemOperation(item::Operation),
    EraseItemOperation(Uuid),
    SaveStamp(Stamp),
    EraseStamp,
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
                let old_offset = offset;
                offset += head.0;
                OperationMetadata::new (old_offset, head.0, head.1.id().clone(), head.1.head_id().clone())
            }).collect::<Vec<OperationMetadata>>();

        let item_log_file = Self::open_file(item_log_path)
            .or_raise(|| StorageError::backend_open(format!("failed to open operation log file at {item_log_path}")))?;

        let mut offset: u64 = 0;
        let item_positions  = Self::load_all_item_operations_with_length(&item_log_file)?
            .into_iter()
            .map(|item| {
                let old_offset = offset;
                offset += item.0;
                OperationMetadata::new (old_offset, item.0, item.1.id().clone(), item.1.item_id().clone())
            }).collect::<Vec<OperationMetadata>>();

        let file_store = FileStorage {
            stamp_file: stamp_file,
            head_log_file: OperationLogFile::new(head_log_file,head_positions),
            item_log_file: OperationLogFile::new(item_log_file, item_positions),
            in_transaction: false,
            rollback_stack: Vec::new(),
        };

        Ok(file_store)
    }

    fn load_line(file: &mut File, start: u64, length: u64) -> Result<String, StorageError> {
        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in file"))?;

        let mut buffer = vec![0u8; length as usize];
        file.read_exact(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read line from file"))?;

        let line = String::from_utf8(buffer)
            .or_raise(|| StorageError::backend_read("failed to convert bytes to string"))?;

        Ok(line)
    }

    fn load_head_operation(line: &str) -> Result<(u64, head::Operation), StorageError> {
        let length = line.len() + 1;
        let head = head::Operation::try_from(line)
            .or_raise(|| StorageError::data_decode("unable to deserialize line"))?;

        Ok((length as u64, head))
    }

    fn load_all_head_operations_with_length(file: &File) -> Result<Vec<(u64, head::Operation)>, StorageError> {
        let mut file = BufReader::new(file);
        file.seek(SeekFrom::Start(0))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        file.lines()
            .map(|line| {
                match line {
                    Ok(l) => Self::load_head_operation(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, head::Operation)>, StorageError>>()
            .or_raise(|| StorageError::data_decode("unable to parse all lines"))
    }

    fn load_item_operation(line: &str) -> Result<(u64, item::Operation), StorageError> {
        let length = line.len() + 1;
        let head = item::Operation::try_from(line)
            .or_raise(|| StorageError::data_decode("unable to deserialize line"))?;

        Ok((length as u64, head))
    }

    fn load_all_item_operations_with_length(file: &File) -> Result<Vec<(u64, item::Operation)>, StorageError> {
        let mut file = BufReader::new(file);
        file.seek(SeekFrom::Start(0))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        file.lines()
            .map(|line| {
                match line {
                    Ok(l) => Self::load_item_operation(&l),
                    Err(e) => Err(e).or_raise(||
                        StorageError::backend_read("unable to load line")
                    ),
                }
            })
            .collect::<Result<Vec<(u64, item::Operation)>, StorageError>>()
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

        let total_steps = self.rollback_stack.len();
        let mut current_step: usize = 0;
        while let Some(rollback_function) = self.rollback_stack.pop() {
            current_step += 1;

            use RollbackFunction::*;
            let rollback_result = match rollback_function {
                SaveHeadOperation(operation) => self.save_head_operation(operation),
                EraseHeadOperation(id) => self.erase_head_operation(&id).map(|_| ()),
                SaveItemOperation(operation) => self.save_item_operation(operation),
                EraseItemOperation(id) => self.erase_item_operation(&id).map(|_| ()),
                SaveStamp(stamp) => self.save_stamp(&stamp),
                EraseStamp => {
                    self.stamp_file.set_len(0)
                        .or_raise(|| StorageError::backend_specific("failed to truncate stamp file"))
                },
            };

            if let Err(e) = rollback_result {
                self.rollback_stack.clear();
                bail!(e.raise(StorageError::transaction_rollback_partial(format!(
                    "failed to rollback step {current_step} of {total_steps} of the transaction"
                ))));
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
                self.rollback_stack.push(RollbackFunction::EraseStamp);
            } else {
                let current_stamp = self.load_stamp()?;
                self.rollback_stack.push(RollbackFunction::SaveStamp(current_stamp));
            }
        }

        self.stamp_file.set_len(0)
            .or_raise(|| StorageError::backend_specific("failed to truncate stamp file"))?;

        self.stamp_file.seek(SeekFrom::Start(0))
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp file"))?;

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
            .or_raise(|| StorageError::backend_specific("failed to seek position in stamp file"))?;

        let mut stamp_buf = String::new();
        self.stamp_file.read_to_string(&mut stamp_buf)
            .or_raise(|| StorageError::backend_read("failed to read stamp file"))?;
        let stamp = Stamp::from_str(&stamp_buf)
            .or_raise(|| StorageError::stamp_invalid(format!("failed to parse stamp '{}'", &stamp_buf)))?;

        Ok(stamp)
    }

    fn save_head_operation(&mut self, operation: head::Operation) -> Result<(), StorageError> {
        let id = operation.id().clone();
        let head_id = operation.head_id().clone();
        let line: String = operation.into();

        let file = &mut self.head_log_file.file;
        let operation_positions = &mut self.head_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(&id, |i| i.id) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("head operation with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].offset + operation_positions[i].length)
            .unwrap_or(0);
        let line_len = line.len() as u64;
        let medadata = OperationMetadata::new(start, line_len, id.clone(), head_id);

        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        if index == operation_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head operation '{id}' to file")))?;
            operation_positions.push(medadata);
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from head operation file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate head operation file"))?;

            file.seek(SeekFrom::Start(start))
                .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write head operation '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head operation file"))?;

            operation_positions.iter_mut()
                .skip(index)
                .for_each(|m| m.offset += line_len);

            operation_positions.insert(index, medadata);
        }

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::EraseHeadOperation(id));
        }

        Ok(())
    }

    fn save_head_tombstone(&mut self, tombstone: head::Tombstone) -> Result<(), StorageError> {
        todo!()
    }

    fn load_head_operations(&mut self) -> Result<Vec<head::Operation>, StorageError> {
        Ok(Self::load_all_head_operations_with_length(&self.head_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn load_head_tombstones(&mut self) -> Result<Vec<head::Tombstone>, StorageError> {
        todo!()
    }

    fn load_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<head::Operation>, StorageError> {
        self.head_log_file.operation_positions.iter()
            .filter(|meta| meta.associated_id == *head_id)
            .map(|meta| {
                Self::load_line(&mut self.head_log_file.file, meta.offset, meta.length-1)
                    .and_then(|line| Self::load_head_operation(&line))
                    .and_then(|pair| Ok(pair.1))
            })
            .collect()
    }

    fn load_associated_head_tombstone(&mut self, head_id: &Uuid) -> Result<Option<head::Tombstone>, StorageError> {
        todo!()
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<head::Operation, StorageError> {
        let file = &mut self.head_log_file.file;
        let operation_positions = &mut self.head_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(id, |i| i.id) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of head operation with id '{id}'")
            )),
        };

        let meta = operation_positions.remove(index);

        file.seek(SeekFrom::Start(meta.offset))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read head operation from file"))?;

        let head_slice = buffer.get(0 .. (meta.length-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < operation_positions.len() {
            let val = buffer.get((meta.offset + meta.length) as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let head_operation = Self::load_head_operation(head_slice)
            .or_raise(|| StorageError::backend_read("failed to read head operation from file"))?
            .1;

        file.set_len(meta.offset)
            .or_raise(|| StorageError::backend_specific("failed to truncate head operation file"))?;

        file.seek(SeekFrom::Start(meta.offset))
            .or_raise(|| StorageError::backend_specific("failed to seek position in head operation file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to head operation file"))?;
            operation_positions.iter_mut()
                .skip(index)
                .for_each(|m| m.offset -= meta.length);
        };

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::SaveHeadOperation(head_operation.clone()));
        }

        Ok(head_operation)
    }

    fn erase_head_tombstone(&mut self, id: &Uuid) -> Result<head::Tombstone, StorageError> {
        todo!()
    }

    fn save_item_operation(&mut self, operation: item::Operation) -> Result<(), StorageError> {
        let id = operation.id().clone();
        let item_id = operation.item_id().clone();
        let line: String = operation.into();

        let file = &mut self.item_log_file.file;
        let operation_positions = &mut self.item_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(&id, |i| i.id) {
            Ok(_) => bail!(StorageError::backend_specific(
                format!("item operation with id '{id}' is already in file")
            )),
            Err(i) => i,
        };

        let start = index
            .checked_sub(1)
            .map(|i| operation_positions[i].offset + operation_positions[i].length)
            .unwrap_or(0);
        let line_len = line.len() as u64;
        let medadata = OperationMetadata::new(start, line_len, id.clone(), item_id);


        file.seek(SeekFrom::Start(start))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        if index == operation_positions.len() {
            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item operation '{id}' to file")))?;
            operation_positions.push(medadata);
        } else {
            let mut remainder = String::new();
            file.read_to_string(&mut remainder)
                .or_raise(|| StorageError::backend_read("failed to read remainder from item operation file"))?;

            file.set_len(start)
                .or_raise(|| StorageError::backend_specific("failed to truncate item operation file"))?;

            file.seek(SeekFrom::Start(start))
                .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

            file.write(line.as_bytes())
                .or_raise(|| StorageError::backend_write(format!("failed to write item operation '{id}' to file")))?;

            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item operation file"))?;

            operation_positions.iter_mut()
                .skip(index)
                .for_each(|m| m.offset += line_len);

            operation_positions.insert(index, medadata);
        }

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::EraseItemOperation(id));
        }

        Ok(())
    }

    fn save_item_tombstone(&mut self, tombstone: item::Tombstone) -> Result<(), StorageError> {
        todo!()
    }

    fn load_item_operations(&mut self) -> Result<Vec<item::Operation>, StorageError> {
        Ok(Self::load_all_item_operations_with_length(&self.item_log_file.file)?
            .into_iter().map(|t| t.1)
            .collect()
        )
    }

    fn load_item_tombstones(&mut self) -> Result<Vec<item::Tombstone>, StorageError> {
        todo!()
    }

    fn load_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<item::Operation>, StorageError> {
        self.item_log_file.operation_positions.iter()
            .filter(|meta| meta.associated_id == *item_id)
            .map(|meta| {
                Self::load_line(&mut self.item_log_file.file, meta.offset, meta.length-1)
                    .and_then(|line| Self::load_item_operation(&line))
                    .and_then(|pair| Ok(pair.1))
            })
            .collect()
    }

    fn load_associated_item_tombstone(&mut self, item_id: &Uuid) -> Result<Option<item::Tombstone>, StorageError> {
        todo!()
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<item::Operation, StorageError> {
        let file = &mut self.item_log_file.file;
        let operation_positions = &mut self.item_log_file.operation_positions;

        let index = match operation_positions.binary_search_by_key(id, |i| i.id) {
            Ok(i) => i,
            Err(_) => bail!(StorageError::backend_specific(
                format!("unable to find position of item operation with id '{id}'")
            )),
        };

        let meta = operation_positions.remove(index);

        file.seek(SeekFrom::Start(meta.offset))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)
            .or_raise(|| StorageError::backend_read("failed to read item operation from file"))?;

        let item_slice = buffer.get(0 .. (meta.length-1) as usize)
            .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;

        let remainder_slice = if index < operation_positions.len() {
            let val = buffer.get((meta.offset + meta.length) as usize .. buffer.len())
                .ok_or_raise(|| StorageError::backend_specific("failed to calculate indices for string split properly"))?;
            Some(val)
        } else {
            None
        };

        let item_operation = Self::load_item_operation(item_slice)
            .or_raise(|| StorageError::backend_read("failed to read item operation from file"))?
            .1;

        file.set_len(meta.offset)
            .or_raise(|| StorageError::backend_specific("failed to truncate item operation file"))?;

        file.seek(SeekFrom::Start(meta.offset))
            .or_raise(|| StorageError::backend_specific("failed to seek position in item operation file"))?;

        if let Some(remainder) = remainder_slice {
            file.write(remainder.as_bytes())
                .or_raise(|| StorageError::backend_write("failed to write remainder to item operation file"))?;
            operation_positions.iter_mut()
                .skip(index)
                .for_each(|m| m.offset -= meta.length);
        };

        if self.in_transaction {
            self.rollback_stack.push(RollbackFunction::SaveItemOperation(item_operation.clone()));
        }

        Ok(item_operation)
    }

    fn erase_item_tombstone(&mut self, id: &Uuid) -> Result<item::Tombstone, StorageError> {
        todo!()
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

        let head = head::Operation::Creation {
            id: uuid::Uuid::now_v7(),
            history: itc::EventTree::zero(),
            template_id: Some(uuid::Uuid::new_v4()),
            name: "test test".into(),
            description: Some("this is a description".into())
        };

        file_store.save_head_operation(head).unwrap();
    }
}
