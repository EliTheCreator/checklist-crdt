use core::future::Future;
use exn::{OptionExt, Result, ResultExt, bail};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use sqlx::pool::PoolConnection;
use sqlx::{Connection, Row, Sqlite, Transaction};
use sqlx::sqlite::SqliteRow;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::{ErrorKind, StorageError};
use crate::persistence::storage::Store;


const HEAD_OPERATION_TABLE: &str = "head_operation";
const HEAD_TOMBSTONE_TABLE: &str = "head_tombstone";
const ITEM_OPERATION_TABLE: &str = "item_operation";
const ITEM_TOMBSTONE_TABLE: &str = "item_tombstone";


pub trait BlockOn {
    fn block_on<F: Future>(&self, fut: F) -> F::Output;
}


pub struct SqliteStorage<'a, B: BlockOn> {
    executor: &'a B,
    sqlite_connection: &'a mut PoolConnection<Sqlite>,
    transaction: Option<Transaction<'a, Sqlite>>
}

impl<'a, B: BlockOn> SqliteStorage<'a, B> {
    pub fn new(sqlite_connection: &'a mut PoolConnection<Sqlite>, executor: &'a B) -> Result<Self, StorageError> {
        let _ = executor.block_on(
            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS stamp (
                        id INTEGER PRIMARY KEY CHECK (id = 0),
                        stamp BLOB NOT NULL
                    ) WITHOUT ROWID;
                    "#
                )
                .execute(&mut **sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to create stamp table"))?;

        let _ = executor.block_on(
            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS head_operation (
                        id BLOB PRIMARY KEY NOT NULL,
                        history BLOB NOT NULL,
                        secondary_id BLOB NOT NULL,
                        variant INTEGER NOT NULL,
                        payload BLOB NOT NULL
                    ) WITHOUT ROWID;
                    "#
                )
                .execute(&mut **sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to create head_operation table"))?;

        let _ = executor.block_on(
            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS head_tombstone (
                        id BLOB PRIMARY KEY NOT NULL,
                        history BLOB NOT NULL,
                        secondary_id BLOB NOT NULL,
                        variant INTEGER NOT NULL,
                        payload BLOB NOT NULL
                    ) WITHOUT ROWID;
                    "#
                )
                .execute(&mut **sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to create head_operation table"))?;

        let _ = executor.block_on(
            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS item_operation (
                        id BLOB PRIMARY KEY NOT NULL,
                        history BLOB NOT NULL,
                        secondary_id BLOB NOT NULL,
                        variant INTEGER NOT NULL,
                        payload BLOB NOT NULL
                    ) WITHOUT ROWID;
                    "#
                )
                .execute(&mut **sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to create item_operation table"))?;

        Ok(Self { executor, sqlite_connection, transaction: None })
    }

    fn usize_to_vec_u8(mut num: usize) -> Vec<u8> {
        let mask: usize = 1<<7 - 1;

        let mut binary = Vec::new();
        while num != 0 {
            binary.push((num&mask) as u8);
            num >>= 7;
        }

        binary.iter_mut().rev().skip(1).for_each(|n| {*n |= 1<<7;});

        binary
    }

    fn str_to_vec_u8(string: &str) -> Vec<u8> {
        let string_as_bytes = string.to_string().into_bytes();
        let length = string_as_bytes.len();

        let mut bytes = Self::usize_to_vec_u8(length);
        bytes.extend(string_as_bytes);

        bytes
    }

    fn position_to_vec_u8(position: &FractionalIndex) -> Vec<u8> {
        let position_as_bytes = position.as_bytes();
        let length = position_as_bytes.len();

        let mut bytes = Self::usize_to_vec_u8(length);
        bytes.extend(position_as_bytes);

        bytes
    }

    fn head_to_payload(operation: &HeadOperation) -> Vec<u8> {
        use HeadOperation::*;
        match operation {
            Creation { name, description, .. } => {
                let mut bytes = Self::str_to_vec_u8(name);
                let description = description.as_deref().unwrap_or_default();
                let description_as_bytes = Self::str_to_vec_u8(description);
                bytes.extend(description_as_bytes);

                bytes
            },
            NameUpdate { name, ..} => {
                Self::str_to_vec_u8(name)
            },
            DescriptionUpdate {description, .. } => {
                let description = description.as_deref().unwrap_or_default();
                Self::str_to_vec_u8(description)
            },
            CompletedUpdate { completed, ..} => {
                vec![completed.clone() as u8,]
            },
            Deletion { .. }  => Vec::new(),
            Tombstone { template_id, name, description, completed, .. }  => {
                let mut bytes = if let Some(template_id) = template_id {
                    let template_id_bytes= template_id.as_bytes();
                    let mut bytes = Self::usize_to_vec_u8(template_id_bytes.len());
                    bytes.extend_from_slice(template_id_bytes);
                    bytes
                } else {
                    vec![0,]
                };

                bytes.extend(Self::str_to_vec_u8(name));

                let description = description.as_deref().unwrap_or_default();
                let description_as_bytes = Self::str_to_vec_u8(description);
                bytes.extend(description_as_bytes);

                bytes.push(completed.clone() as u8);

                bytes
            },
        }
    }

    fn head_to_variant_id(operation: &HeadOperation) -> u8 {
        use HeadOperation::*;
        match operation {
            Creation { .. } => 0,
            NameUpdate { .. } => 1,
            DescriptionUpdate { .. } => 2,
            CompletedUpdate { .. } => 3,
            Deletion { .. } => 4,
            Tombstone { .. }  => 5,
        }
    }

    fn item_to_payload(operation: &ItemOperation) -> Vec<u8> {
        use ItemOperation::*;
        match operation {
            Creation { name, position, .. } => {
                let mut bytes = Self::str_to_vec_u8(name);
                bytes.extend(position.as_bytes());

                bytes
            },
            NameUpdate { name, ..} => {
                Self::str_to_vec_u8(name)
            },
            PositionUpdate {position, .. } => {
                position.as_bytes().into()
            },
            CheckedUpdate { checked, ..} => {
                vec![checked.clone() as u8,]
            },
            Deletion { .. }  => Vec::new(),
            Tombstone { head_id, name, position, checked, .. } => {
                let mut bytes = Vec::from(head_id.as_bytes());
                bytes.extend(Self::str_to_vec_u8(name));
                bytes.extend(Self::position_to_vec_u8(position));
                bytes.push(checked.clone() as u8);

                bytes
            }
        }
    }

    fn item_to_variant_id(operation: &ItemOperation) -> u8 {
        use ItemOperation::*;
        match operation {
            Creation { .. } => 0,
            NameUpdate { .. } => 1,
            PositionUpdate { .. } => 2,
            CheckedUpdate { .. } => 3,
            Deletion { .. } => 4,
            Tombstone { .. } => 5,
        }
    }

    fn usize_from_payload(payload: &[u8]) -> (usize, usize) {
        let mask: u8 = 1<<7 - 1;
        let mut bytes = 0;
        let mut num = 0;

        for byte in payload {
            let value = (*byte&mask) as usize;
            num += value << (bytes*7);
            bytes += 1;

            if byte>>7 == 0 {
                break
            }
        }

        (bytes, num)
    }

    fn sqlite_row_to_head_operation(row: SqliteRow) -> Result<HeadOperation, StorageError> {
        let id_bytes: &[u8] = row.try_get("id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'id' field"))?;
        let id = Uuid::from_slice(id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let history_bytes: &[u8] = row.try_get("history")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'history' field"))?;
        let history = EventTree::try_from(history_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode event tree"))?;

        let variant: u8 = row.try_get("variant")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'variant' field"))?;

        let associated_id_bytes: &[u8] = row.try_get("associated_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'associated_id' field"))?;
        let associated_id = if associated_id_bytes.len() != 0 {
            Some(Uuid::from_slice(associated_id_bytes)
                .or_raise(|| StorageError::data_decode("unable to decode uuid"))?)
        } else {
            None
        };

        let payload: &[u8] = row.try_get("payload")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'payload' field"))?;

        match variant {
            0 => {
                let (name_offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(name_offset..(name_offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                let desc_length_slice = payload.get((name_offset+name_length)..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let (mut desc_offset, desc_length) = Self::usize_from_payload(desc_length_slice);
                desc_offset += name_offset+name_length;

                let description = if desc_length != 0 {
                    let desc_slice = payload.get(desc_offset..(desc_length+desc_offset))
                        .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                    Some(String::from_utf8(desc_slice.to_vec())
                        .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?)
                } else {
                    None
                };

                Ok(HeadOperation::Creation { id, history, template_id: associated_id, name, description })
            },
            1 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let (offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(offset..(name_length+offset))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                Ok(HeadOperation::NameUpdate { id, history, head_id, name })
            },
            2 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let (desc_offset, desc_length) = Self::usize_from_payload(payload);

                let description = if desc_length != 0 {
                    let desc_slice = payload.get(desc_offset..(desc_length+desc_offset))
                        .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                    Some(String::from_utf8(desc_slice.to_vec())
                        .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?)
                } else {
                    None
                };

                Ok(HeadOperation::DescriptionUpdate { id, history, head_id, description })
            },
            3 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let completed = payload.get(0)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let completed = *completed == 1;

                Ok(HeadOperation::CompletedUpdate { id, history, head_id, completed })
            },
            4 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                Ok(HeadOperation::Deletion { id, history, head_id })
            },
            5 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let (template_id_offset, template_id_length) = Self::usize_from_payload(payload);
                let template_id_slice = payload.get(template_id_offset..(template_id_offset+template_id_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let template_id = if template_id_slice.len() != 0 {
                    Some(Uuid::from_slice(template_id_slice)
                        .or_raise(|| StorageError::data_decode("unable to decode uuid"))?)
                } else {
                    None
                };

                let name_length_slice = payload.get((template_id_offset+template_id_length)..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let (mut name_offset, name_length) = Self::usize_from_payload(name_length_slice);
                name_offset += template_id_offset + template_id_length;

                let name_slice = payload.get(name_offset..(name_offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                let desc_length_slice = payload.get((name_offset+name_length)..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let (mut desc_offset, desc_length) = Self::usize_from_payload(desc_length_slice);
                desc_offset += name_offset+name_length;

                let description = if desc_length != 0 {
                    let desc_slice = payload.get(desc_offset..(desc_length+desc_offset))
                        .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                    Some(String::from_utf8(desc_slice.to_vec())
                        .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?)
                } else {
                    None
                };

                let completed = payload.get(desc_offset+desc_length)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let completed = *completed == 1;

                Ok(HeadOperation::Tombstone { id, history, head_id, template_id, name, description, completed })
            },
            _ => bail!(StorageError::data_decode("encountered unknown head operation kind"))
        }
    }

    fn sqlite_row_to_item_operation(row: SqliteRow) -> Result<ItemOperation, StorageError> {
        let id_bytes: &[u8] = row.try_get("id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'id' field"))?;
        let id = Uuid::from_slice(id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let history_bytes: &[u8] = row.try_get("history")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'history' field"))?;
        let history = EventTree::try_from(history_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode event tree"))?;

        let variant: u8 = row.try_get("variant")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'variant' field"))?;

        let associated_id_bytes: &[u8] = row.try_get("associated_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'associated_id' field"))?;
        let associated_id = Uuid::from_slice(associated_id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let payload: &[u8] = row.try_get("payload")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'payload' field"))?;

        match variant {
            0 => {
                let (offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(offset..(offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                let position_slice = payload.get(offset+name_length..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let position = FractionalIndex::from_bytes(position_slice.to_vec());

                Ok(ItemOperation::Creation { id, history, head_id: associated_id, name, position })
            },
            1 => {
                let (offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(offset..(offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                Ok(ItemOperation::NameUpdate { id, history, item_id: associated_id, name })
            },
            2 => {
                let position = FractionalIndex::from_bytes(payload.to_vec());

                Ok(ItemOperation::PositionUpdate { id, history, item_id: associated_id, position })
            },
            3 => {
                let checked = payload.get(0)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let checked = *checked == 1;

                Ok(ItemOperation::CheckedUpdate { id, history, item_id: associated_id, checked })
            },
            4 => {
                Ok(ItemOperation::Deletion { id, history, item_id: associated_id })
            },
            5 => {
                let uuid_length: usize = 16;
                let item_id_slice = payload.get(0..uuid_length)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let item_id= Uuid::from_slice(item_id_slice)
                        .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

                let name_length_slice = payload.get(uuid_length..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let (mut name_offset, name_length) = Self::usize_from_payload(name_length_slice);
                name_offset += uuid_length;

                let name_slice = payload.get(name_offset..(name_offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                let position_length_slice = payload.get((name_offset+name_length)..)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let (mut position_offset, position_length) = Self::usize_from_payload(position_length_slice);
                position_offset += name_offset+name_length;

                let position_slice = payload.get(position_offset..(position_offset+position_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let position = FractionalIndex::from_bytes(position_slice.to_vec());

                let checked = payload.get(position_offset+position_length)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let checked = *checked == 1;


                Ok(ItemOperation::Tombstone { id, history, head_id: associated_id, item_id, name, position, checked })
            }
            _ => bail!(StorageError::data_decode("encountered unknown item operation kind"))
        }
    }

    fn save_operation_to(
        &mut self,
        table_name: &str,
        id: &[u8],
        history: Box<[u8]>,
        secondary_id: &[u8],
        variant: u8,
        payload: Vec<u8>
    ) -> Result<(), StorageError> {
        let query = sqlx::query("
                INSERT INTO ?
                (id, history, secondary_id, variant, payload)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
            ")
            .bind(table_name)
            .bind(id)
            .bind(history)
            .bind(secondary_id)
            .bind(variant)
            .bind(payload);

        let _ = if let Some(transaction) = &mut self.transaction {
            self.executor.block_on(query.execute(&mut **transaction))
        } else {
            self.executor.block_on(query.execute(&mut **self.sqlite_connection))
        }
        .or_raise(|| StorageError::backend_write(
            format!("failed save operation to '{table_name}'")
        ))?;
        
        Ok(())
    }

    fn load_operations_from(&mut self, table_name: &str) -> Result<Vec<SqliteRow>, StorageError> {
        let query = sqlx::query(
                r#"
                    SELECT id, history, secondary_id, variant, payload
                    FROM ?
                "#
            )
            .bind(table_name);

        if let Some(transaction) = &mut self.transaction {
            self.executor.block_on(query.fetch_all(&mut **transaction))
        } else {
            self.executor.block_on(query.fetch_all(&mut **self.sqlite_connection))
        }
        .or_raise(|| StorageError::backend_read(
            format!("failed load operations from '{table_name}'"))
        )
    }

    fn load_head_operations_from(&mut self, table_name: &str) -> Result<Vec<HeadOperation>, StorageError> {        
        self.load_operations_from(table_name)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn load_item_operations_from(&mut self, table_name: &str) -> Result<Vec<ItemOperation>, StorageError> {        
        self.load_operations_from(table_name)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_item_operation(row))
            .collect()
    }

    fn load_associated_operations_from(
        &mut self,
        table_name: &str,
        associated_id: &Uuid,
    ) -> Result<Vec<SqliteRow>, StorageError> {
        let query = sqlx::query("
                SELECT id, history, secondary_id, variant, payload
                FROM ?
                WHERE id = ? OR secondary_id = ?
            ")
            .bind(table_name)
            .bind(associated_id.as_bytes().as_slice())
            .bind(associated_id.as_bytes().as_slice());

        if let Some(transaction) = &mut self.transaction {
            self.executor.block_on(query.fetch_all(&mut **transaction))
        } else {
            self.executor.block_on(query.fetch_all(&mut **self.sqlite_connection))
        }
        .or_raise(|| StorageError::backend_read(format!(
            "failed load associated operations to '{associated_id}' from '{table_name}'")
        ))
    }

    fn load_associated_head_operations_from(
        &mut self,
        table_name: &str,
        head_id: &Uuid,
    ) -> Result<Vec<HeadOperation>, StorageError> {      
        self.load_associated_operations_from(table_name, head_id)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn load_associated_item_operations_from(
        &mut self,
        table_name: &str,
        item_id: &Uuid,
    ) -> Result<Vec<ItemOperation>, StorageError> {      
        self.load_associated_operations_from(table_name, item_id)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_item_operation(row))
            .collect()
    }

    fn erase_operation_from(
        &mut self,
        table_name: &str,
        id: &Uuid        
    ) -> Result<SqliteRow, StorageError> {
        let query = sqlx::query("
                DELETE FROM ?
                WHERE ID = ?
                RETURNING id, history, secondary_id, variant, payload
            ")
            .bind(table_name)
            .bind(id.as_bytes().as_slice());

        if let Some(transaction) = &mut self.transaction {
            self.executor.block_on(query.fetch_optional(&mut **transaction))
        } else {
            self.executor.block_on(query.fetch_optional(&mut **self.sqlite_connection))
        }
        .or_raise(|| StorageError::backend_delete(
            format!("failed to delete operation from {table_name}"))
        )?
        .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))
    }
}

impl<'a, B: BlockOn> Store<'a> for SqliteStorage<'a, B> {
    fn start_transaction(&'a mut self) -> Result<bool, StorageError> {
        if self.transaction.is_some() {
            return Ok(false)
        }

        self.transaction = Some(self.executor.block_on(
                self.sqlite_connection.begin()
            )
            .or_raise(|| StorageError::backend_specific("failed to start transaction"))?);
        Ok(true)
    }

    fn abort_transaction(&mut self) -> Result<bool, StorageError> {
        if let Some(transaction) = self.transaction.take() {
            self.executor.block_on(
                    transaction.rollback()
                )
                .or_raise(|| StorageError::transaction_rollback_partial("failed to abort transaction"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn commit_transaction(&mut self) -> Result<bool, StorageError> {
        if let Some(transaction) = self.transaction.take() {
            self.executor.block_on(
                    transaction.commit()
                )
                .or_raise(|| StorageError::backend_specific("failed to commit transaction"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        let query = sqlx::query(
                r#"
                    INSERT INTO stamp (id, stamp)
                    VALUES (0, ?)
                    ON CONFLICT(ID)
                    DO UPDATE SET stamp = excluded.stamp
                "#
            )
            .bind(Into::<Box<[u8]>>::into(stamp));

        let _ = self.executor.block_on(
                query
                    .execute(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_write("failed to save stamp"))?;

        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        let query = sqlx::query(
                r#"
                    SELECT stamp
                    FROM stamp
                    WHERE id = 0
                "#
            );

        let row = self.executor.block_on(
                query
                    .fetch_optional(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_read("failed to load stamp"))?
            .ok_or_raise(|| StorageError::stamp_none("expected stamp record, found none"))?;

        let stamp_slice: &[u8] = row.try_get("stamp")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'stamp' field"))?;

        Stamp::try_from(stamp_slice)
            .or_raise(|| StorageError::stamp_invalid("unable to parse stamp"))
    }

    fn save_head_operation(&mut self, operation: HeadOperation) -> Result<(), StorageError> {
        let table_name = if let HeadOperation::Tombstone { .. } = operation {
            HEAD_TOMBSTONE_TABLE
        } else {
            HEAD_OPERATION_TABLE
        };

        let secondary_id = match &operation {
            HeadOperation::Creation { template_id: None, .. } => {
                &[]
            },
            HeadOperation::Creation { template_id: Some(id), .. } => {
                id.as_bytes().as_slice()
            },
            _ => operation.head_id().as_bytes().as_slice()
        };

        self.save_operation_to(
            table_name,
            operation.id().as_bytes().as_slice(),
            Into::<Box<[u8]>>::into(operation.history()),
            secondary_id,
            Self::head_to_variant_id(&operation),
            Self::head_to_payload(&operation),
        )
    }

    fn load_all_head_operations(&mut self) -> Result<Vec<HeadOperation>, StorageError> {
        let mut operations = Vec::new();
        for table_name in [HEAD_TOMBSTONE_TABLE, HEAD_OPERATION_TABLE] {
            operations.extend(self.load_head_operations_from(table_name)?);
        }
        
        Ok(operations)
    }

    fn load_all_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<HeadOperation>, StorageError> {
        let mut operations = Vec::new();
        for table_name in [HEAD_TOMBSTONE_TABLE, HEAD_OPERATION_TABLE] {
            operations.extend(self.load_associated_head_operations_from(table_name, head_id)?);
        }
        
        Ok(operations)
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError> {
        let row = match self.erase_operation_from(HEAD_OPERATION_TABLE, id) {
            Ok(r) => r,
            Err(e) if e.kind == ErrorKind::BackendDelete => {
                self.erase_operation_from(HEAD_TOMBSTONE_TABLE, id)?
            },
            Err(e) => Err(e)?,
        };

        Self::sqlite_row_to_head_operation(row)
    }

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError> {
        self.save_operation_to(
            ITEM_OPERATION_TABLE,
            operation.id().as_bytes().as_slice(),
            Into::<Box<[u8]>>::into(operation.history()),
            operation.item_id().as_bytes().as_slice(),
            Self::item_to_variant_id(&operation),
            Self::item_to_payload(&operation),
        )
    }

    fn load_all_item_operations(&mut self) -> Result<Vec<ItemOperation>, StorageError> {
        let mut operations = Vec::new();
        for table_name in [ITEM_TOMBSTONE_TABLE, ITEM_OPERATION_TABLE] {
            operations.extend(self.load_item_operations_from(table_name)?);
        }
        
        Ok(operations)
    }

    fn load_all_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<ItemOperation>, StorageError> {
        let mut operations = Vec::new();
        for table_name in [ITEM_TOMBSTONE_TABLE, ITEM_OPERATION_TABLE] {
            operations.extend(self.load_associated_item_operations_from(table_name, item_id)?);
        }
        
        Ok(operations)
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError> {
        let row = match self.erase_operation_from(ITEM_OPERATION_TABLE, id) {
            Ok(r) => r,
            Err(e) if e.kind == ErrorKind::BackendDelete => {
                self.erase_operation_from(ITEM_TOMBSTONE_TABLE, id)?
            },
            Err(e) => Err(e)?,
        };

        Self::sqlite_row_to_item_operation(row)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
    use tokio::runtime::Runtime;
    use std::{future::Future, str::FromStr};


    pub struct TokioBlockOn {
        runtime: Runtime,
    }

    impl TokioBlockOn {
        pub fn new() -> Self {
            Self { runtime: Runtime::new().unwrap() }
        }
    }

    impl BlockOn for TokioBlockOn {
        fn block_on<F: Future>(&self, fut: F) -> F::Output {
            self.runtime.block_on(fut)
        }
    }

    #[test]
    fn basic_in_memory_init() {
        let executor = TokioBlockOn::new();
        let sql_connection_options = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        let sql_connection_options = sql_connection_options.in_memory(true);
        let sqlite_pool = executor
            .block_on(SqlitePool::connect_with(sql_connection_options))
            .unwrap();
        let mut connection = executor.block_on(sqlite_pool.acquire()).unwrap();
        let mut storage = SqliteStorage::new(&mut connection, &executor).unwrap();

        storage.save_stamp(&Stamp::seed()).unwrap();
        let _ = storage.load_stamp().unwrap();
        executor.block_on(async {drop(storage)});
        executor.block_on(async {drop(connection)});
    }
}
