use core::future::Future;
use exn::{OptionExt, Result, ResultExt, bail};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use sqlx::{Pool, Row, Sqlite, Transaction};
use sqlx::sqlite::SqliteRow;
use uuid::Uuid;

use crate::persistence::model::checklist::{head, item};
use crate::persistence::{ErrorKind, StorageError};
use crate::persistence::storage::Store;


macro_rules! execute {
    ($this:expr, $query:expr, $method:ident) => {
        if let Some(transaction) = &mut $this.transaction {
            $this.executor.block_on($query.$method(&mut **transaction))
        } else {
            $this.executor.block_on(async {
                let mut connection = $this.sqlite_pool.acquire().await.unwrap();
                $query.$method(&mut *connection).await
            })
        }
    };
}


const HEAD_OPERATION_TABLE: &str = "head_operation";
const HEAD_TOMBSTONE_TABLE: &str = "head_tombstone";
const ITEM_OPERATION_TABLE: &str = "item_operation";
const ITEM_TOMBSTONE_TABLE: &str = "item_tombstone";


pub trait BlockOn {
    fn block_on<F: Future>(&self, fut: F) -> F::Output;
}


pub struct SqliteStorage<'a, B: BlockOn> {
    executor: &'a B,
    sqlite_pool: &'a mut Pool<Sqlite>,
    transaction: Option<Transaction<'a, Sqlite>>
}

impl<'a, B: BlockOn> SqliteStorage<'a, B> {
    pub fn new(sqlite_pool: &'a mut Pool<Sqlite>, executor: &'a B) -> Result<Self, StorageError> {
        let _ = executor.block_on(async {
                let mut connection = sqlite_pool.acquire().await.unwrap();
                sqlx::query("
                        CREATE TABLE IF NOT EXISTS stamp (
                            id INTEGER PRIMARY KEY CHECK (id = 0),
                            stamp BLOB NOT NULL
                        ) WITHOUT ROWID;
                    ")
                    .execute(&mut *connection).await
            })
            .or_raise(|| StorageError::backend_specific("failed to create stamp table"))?;

        for table_name in [HEAD_OPERATION_TABLE, ITEM_OPERATION_TABLE] {
            executor.block_on(async {
                    let mut connection = sqlite_pool.acquire().await.unwrap();
                    sqlx::query(&format!("
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                id BLOB PRIMARY KEY NOT NULL,
                                history BLOB NOT NULL,
                                secondary_id BLOB NOT NULL,
                                variant INTEGER NOT NULL,
                                payload BLOB NOT NULL
                            ) WITHOUT ROWID;
                        "))
                        .execute(&mut *connection).await
                })
                .or_raise(|| StorageError::backend_specific(format!(
                    "failed to create '{table_name}' table"
                )))?;
        }

        let _ = executor.block_on(async {
                let mut connection = sqlite_pool.acquire().await.unwrap();
                sqlx::query(&format!("
                        CREATE TABLE IF NOT EXISTS {HEAD_TOMBSTONE_TABLE} (
                            id BLOB PRIMARY KEY NOT NULL,
                            history BLOB NOT NULL,
                            head_id BLOB NOT NULL,
                            template_id BLOB,
                            name TEXT NOT NULL,
                            description TEXT,
                            completed BOOLEAN NOT NULL
                        ) WITHOUT ROWID;
                    "))
                    .execute(&mut *connection).await
            })
            .or_raise(|| StorageError::backend_specific(format!(
                "failed to create '{HEAD_TOMBSTONE_TABLE}' table")
            ))?;

        let _ = executor.block_on(async {
                let mut connection = sqlite_pool.acquire().await.unwrap();
                sqlx::query(&format!("
                        CREATE TABLE IF NOT EXISTS {ITEM_TOMBSTONE_TABLE} (
                            id BLOB PRIMARY KEY NOT NULL,
                            history BLOB NOT NULL,
                            head_id BLOB NOT NULL,
                            item_id BLOB NOT NULL,
                            name TEXT NOT NULL,
                            position BLOB NOT NULL,
                            checked BOOLEAN NOT NULL
                        ) WITHOUT ROWID;
                    "))
                    .execute(&mut *connection).await
            })
            .or_raise(|| StorageError::backend_specific(format!(
                "failed to create '{ITEM_TOMBSTONE_TABLE}' table")
            ))?;

        Ok(Self { executor, sqlite_pool, transaction: None })
    }

    fn usize_to_vec_u8(mut num: usize) -> Vec<u8> {
        let mask: usize = (1<<7) - 1;

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

    fn head_to_payload(operation: &head::Operation) -> Vec<u8> {
        use head::Operation::*;
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
        }
    }

    fn head_to_variant_id(operation: &head::Operation) -> u8 {
        use head::Operation::*;
        match operation {
            Creation { .. } => 0,
            NameUpdate { .. } => 1,
            DescriptionUpdate { .. } => 2,
            CompletedUpdate { .. } => 3,
        }
    }

    fn item_to_payload(operation: &item::Operation) -> Vec<u8> {
        use item::Operation::*;
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
        }
    }

    fn item_to_variant_id(operation: &item::Operation) -> u8 {
        use item::Operation::*;
        match operation {
            Creation { .. } => 0,
            NameUpdate { .. } => 1,
            PositionUpdate { .. } => 2,
            CheckedUpdate { .. } => 3,
        }
    }

    fn usize_from_payload(payload: &[u8]) -> (usize, usize) {
        let mask: u8 = (1<<7) - 1;
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

    fn sqlite_row_to_head_operation(row: SqliteRow) -> Result<head::Operation, StorageError> {
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

        let associated_id_bytes: &[u8] = row.try_get("secondary_id")
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

                Ok(head::Operation::Creation { id, history, template_id: associated_id, name, description })
            },
            1 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let (offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(offset..(name_length+offset))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                Ok(head::Operation::NameUpdate { id, history, head_id, name })
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

                Ok(head::Operation::DescriptionUpdate { id, history, head_id, description })
            },
            3 => {
                let head_id = associated_id
                    .ok_or_raise(|| StorageError::data_decode("expected an id"))?;

                let completed = payload.get(0)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let completed = *completed == 1;

                Ok(head::Operation::CompletedUpdate { id, history, head_id, completed })
            },
            _ => bail!(StorageError::data_decode("encountered unknown head operation kind"))
        }
    }

    fn sqlite_row_to_head_tombstone(row: SqliteRow) -> Result<head::Tombstone, StorageError> {
        let id_bytes: &[u8] = row.try_get("id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'id' field"))?;
        let id = Uuid::from_slice(id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let history_bytes: &[u8] = row.try_get("history")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'history' field"))?;
        let history = EventTree::try_from(history_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode event tree"))?;

        let head_id_bytes: &[u8] = row.try_get("head_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'head_id' field"))?;
        let head_id = Uuid::from_slice(head_id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let template_id_bytes: Option<&[u8]> = row.try_get("template_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'template_id' field"))?;
        let template_id = if let Some(template_id_bytes) = template_id_bytes {
            Some(Uuid::from_slice(template_id_bytes)
                .or_raise(|| StorageError::data_decode("unable to decode uuid"))?)
        } else {
            None
        };

        let name: String = row.try_get("name")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'name' field"))?;

        let description: Option<String> = row.try_get("description")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'description' field"))?;

        let completed: bool = row.try_get("completed")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'completed' field"))?;

        Ok(head::Tombstone { id, history, head_id, template_id, name, description, completed })
    }

    fn sqlite_row_to_item_operation(row: SqliteRow) -> Result<item::Operation, StorageError> {
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

        let associated_id_bytes: &[u8] = row.try_get("secondary_id")
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

                Ok(item::Operation::Creation { id, history, head_id: associated_id, name, position })
            },
            1 => {
                let (offset, name_length) = Self::usize_from_payload(payload);
                let name_slice = payload.get(offset..(offset+name_length))
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let name = String::from_utf8(name_slice.to_vec())
                    .or_raise(|| StorageError::data_decode("unable to convert from [u8] to string"))?;

                Ok(item::Operation::NameUpdate { id, history, item_id: associated_id, name })
            },
            2 => {
                let position = FractionalIndex::from_bytes(payload.to_vec());

                Ok(item::Operation::PositionUpdate { id, history, item_id: associated_id, position })
            },
            3 => {
                let checked = payload.get(0)
                    .ok_or_raise(|| StorageError::data_decode("unable to get enough bytes"))?;
                let checked = *checked == 1;

                Ok(item::Operation::CheckedUpdate { id, history, item_id: associated_id, checked })
            },
            _ => bail!(StorageError::data_decode("encountered unknown item operation kind"))
        }
    }

    fn sqlite_row_to_item_tombstone(row: SqliteRow) -> Result<item::Tombstone, StorageError> {
        let id_bytes: &[u8] = row.try_get("id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'id' field"))?;
        let id = Uuid::from_slice(id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let history_bytes: &[u8] = row.try_get("history")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'history' field"))?;
        let history = EventTree::try_from(history_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode event tree"))?;

        let head_id_bytes: &[u8] = row.try_get("head_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'head_id' field"))?;
        let head_id = Uuid::from_slice(head_id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let item_id_bytes: &[u8] = row.try_get("item_id")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'item_id' field"))?;
        let item_id = Uuid::from_slice(item_id_bytes)
            .or_raise(|| StorageError::data_decode("unable to decode uuid"))?;

        let name: String = row.try_get("name")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'name' field"))?;

        let position_bytes: &[u8] = row.try_get("position")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'position' field"))?;
        let position = FractionalIndex::from_bytes(position_bytes.to_vec());

        let checked: bool = row.try_get("checked")
            .or_raise(|| StorageError::backend_specific("failed to retrieve 'checked' field"))?;

        Ok(item::Tombstone { id, history, head_id, item_id, name, position, checked })
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
        let query_text = format!("
            INSERT INTO {table_name}
            (id, history, secondary_id, variant, payload)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
        ");
        let query = sqlx::query(&query_text)
            .bind(id)
            .bind(history)
            .bind(secondary_id)
            .bind(variant)
            .bind(payload);

        execute!(self, query, execute)
            .or_raise(|| StorageError::backend_write(
                format!("failed save operation to '{table_name}'")
            ))?;

        Ok(())
    }

    fn load_operations_from(&mut self, table_name: &str) -> Result<Vec<SqliteRow>, StorageError> {
        let query = format!("
            SELECT id, history, secondary_id, variant, payload
            FROM {table_name}
        ");

        execute!(self, sqlx::query(&query), fetch_all)
            .or_raise(|| StorageError::backend_read(
                format!("failed load operations from '{table_name}'"))
            )
    }

    fn load_head_operations_from(&mut self, table_name: &str) -> Result<Vec<head::Operation>, StorageError> {
        self.load_operations_from(table_name)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn load_item_operations_from(&mut self, table_name: &str) -> Result<Vec<item::Operation>, StorageError> {
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
        let query_text = format!("
            SELECT id, history, secondary_id, variant, payload
            FROM {table_name}
            WHERE id = ? OR secondary_id = ?
        ");
        let query = sqlx::query(&query_text)
            .bind(associated_id.as_bytes().as_slice())
            .bind(associated_id.as_bytes().as_slice());

        execute!(self, query, fetch_all)
            .or_raise(|| StorageError::backend_read(format!(
                "failed load associated operations to '{associated_id}' from '{table_name}'")
            ))
    }

    fn load_associated_head_operations_from(
        &mut self,
        table_name: &str,
        head_id: &Uuid,
    ) -> Result<Vec<head::Operation>, StorageError> {
        self.load_associated_operations_from(table_name, head_id)?
            .into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn load_associated_item_operations_from(
        &mut self,
        table_name: &str,
        item_id: &Uuid,
    ) -> Result<Vec<item::Operation>, StorageError> {
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
        let query_text = format!("
            DELETE FROM {table_name}
            WHERE ID = ?
            RETURNING id, history, secondary_id, variant, payload
        ");
        let query = sqlx::query(&query_text)
            .bind(id.as_bytes().as_slice());

        execute!(self, query, fetch_optional)
            .or_raise(|| StorageError::backend_delete(
                format!("failed to delete operation from {table_name}"))
            )?
            .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))
    }
}

impl<'a, B: BlockOn> Store for SqliteStorage<'a, B> {
    fn start_transaction(&mut self) -> Result<bool, StorageError> {
        if self.transaction.is_some() {
            return Ok(false)
        }

        self.transaction = Some(self.executor.block_on(self.sqlite_pool.begin())
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
            self.executor.block_on(transaction.commit())
                .or_raise(|| StorageError::backend_specific("failed to commit transaction"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        let query = sqlx::query("
                INSERT INTO stamp (id, stamp)
                VALUES (0, ?)
                ON CONFLICT(ID)
                DO UPDATE SET stamp = excluded.stamp
            ")
            .bind(Into::<Box<[u8]>>::into(stamp));

        let _ = execute!(self, query, execute)
            .or_raise(|| StorageError::backend_write("failed to save stamp"))?;
        // let _ = self.executor.block_on(async {
        //         let mut connection = self.sqlite_pool.acquire().await.unwrap();
        //         query.execute(&mut *connection).await
        //     })
        //     .or_raise(|| StorageError::backend_write("failed to save stamp"))?;

        Ok(())
    }

    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        let query = sqlx::query("
            SELECT stamp
            FROM stamp
            WHERE id = 0
        ");

        let row = self.executor.block_on(async {
                let mut connection = self.sqlite_pool.acquire().await.unwrap();
                query.fetch_optional(&mut *connection).await
            })
            .or_raise(|| StorageError::backend_read("failed to load stamp"))?
            .ok_or_raise(|| StorageError::stamp_none("expected stamp record, found none"))?;

        let stamp_slice: &[u8] = row.try_get("stamp")
        .or_raise(|| StorageError::backend_specific("failed to retrieve 'stamp' field"))?;

        Stamp::try_from(stamp_slice)
            .or_raise(|| StorageError::stamp_invalid("unable to parse stamp"))
    }

    fn save_head_operation(&mut self, operation: head::Operation) -> Result<(), StorageError> {
        let secondary_id = match &operation {
            head::Operation::Creation { template_id: None, .. } => {
                &[]
            },
            head::Operation::Creation { template_id: Some(id), .. } => {
                id.as_bytes().as_slice()
            },
            _ => operation.head_id().as_bytes().as_slice()
        };

        self.save_operation_to(
            HEAD_OPERATION_TABLE,
            operation.id().as_bytes().as_slice(),
            Into::<Box<[u8]>>::into(operation.history()),
            secondary_id,
            Self::head_to_variant_id(&operation),
            Self::head_to_payload(&operation),
        )
    }

    fn save_head_tombstone(&mut self, tombstone: head::Tombstone) -> Result<(), StorageError> {
        let query_text = format!("
            INSERT INTO {HEAD_TOMBSTONE_TABLE}
            (id, history, head_id, template_id, name, description, completed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
        ");
        let query = sqlx::query(&query_text)
            .bind(tombstone.id.as_bytes().as_slice())
            .bind(Into::<Box<[u8]>>::into(tombstone.history))
            .bind(tombstone.head_id.as_bytes().as_slice())
            .bind(tombstone.template_id.as_ref().map(|id| id.as_bytes().as_slice()))
            .bind(tombstone.name)
            .bind(tombstone.description)
            .bind(tombstone.completed);

        let _ = execute!(self, query, execute)
            .or_raise(|| StorageError::backend_write(
                format!("failed save tombstone to '{HEAD_TOMBSTONE_TABLE}'")
            ))?;

        Ok(())
    }

    fn load_head_operations(&mut self) -> Result<Vec<head::Operation>, StorageError> {
        self.load_head_operations_from(HEAD_OPERATION_TABLE)
    }

    fn load_head_tombstones(&mut self) -> Result<Vec<head::Tombstone>, StorageError> {
        let query_text = format!("
                SELECT id, history, head_id, template_id, name, description, completed
                FROM {HEAD_TOMBSTONE_TABLE}
            ");

        execute!(self, sqlx::query(&query_text), fetch_all)
            .or_raise(|| StorageError::backend_read(
                format!("failed load tombstones from '{HEAD_TOMBSTONE_TABLE}'"))
            )?
            .into_iter()
            .map(|row| Self::sqlite_row_to_head_tombstone(row))
            .collect()
    }

    fn load_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<head::Operation>, StorageError> {
        self.load_associated_head_operations_from(HEAD_OPERATION_TABLE, head_id)
    }

    fn load_associated_head_tombstone(&mut self, head_id: &Uuid) -> Result<Option<head::Tombstone>, StorageError> {
        let query_text = format!("
            SELECT id, history, head_id, template_id, name, description, completed
            FROM {HEAD_TOMBSTONE_TABLE}
            WHERE head_id = ?
        ");
        let query = sqlx::query(&query_text)
            .bind(head_id.as_bytes().as_slice());

        let row = execute!(self, query, fetch_optional)
            .or_raise(|| StorageError::backend_read(
                format!("failed load tombstone from '{HEAD_TOMBSTONE_TABLE}'"))
            )?;

        let tombstone = if let Some(row) = row {
            Some(Self::sqlite_row_to_head_tombstone(row)?)
        } else {
            None
        };

        Ok(tombstone)
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<head::Operation, StorageError> {
        let row = match self.erase_operation_from(HEAD_OPERATION_TABLE, id) {
            Ok(r) => r,
            Err(e) if e.kind == ErrorKind::BackendDelete => {
                self.erase_operation_from(HEAD_TOMBSTONE_TABLE, id)?
            },
            Err(e) => Err(e)?,
        };

        Self::sqlite_row_to_head_operation(row)
    }

    fn erase_head_tombstone(&mut self, id: &Uuid) -> Result<head::Tombstone, StorageError> {
        let query_text = format!("
            DELETE FROM {HEAD_TOMBSTONE_TABLE}
            WHERE ID = ?
            RETURNING id, history, head_id, template_id, name, description, completed
        ");
        let query = sqlx::query(&query_text)
            .bind(id.as_bytes().as_slice());

        let row = execute!(self, query, fetch_optional)
            .or_raise(|| StorageError::backend_delete(
                format!("failed to delete tombstone from {HEAD_TOMBSTONE_TABLE}"))
            )?
            .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))?;

        Ok(Self::sqlite_row_to_head_tombstone(row)?)
    }

    fn save_item_operation(&mut self, operation: item::Operation) -> Result<(), StorageError> {
        self.save_operation_to(
            ITEM_OPERATION_TABLE,
            operation.id().as_bytes().as_slice(),
            Into::<Box<[u8]>>::into(operation.history()),
            operation.item_id().as_bytes().as_slice(),
            Self::item_to_variant_id(&operation),
            Self::item_to_payload(&operation),
        )
    }

    fn save_item_tombstone(&mut self, tombstone: item::Tombstone) -> Result<(), StorageError> {
        let query_text = format!("
            INSERT INTO {HEAD_TOMBSTONE_TABLE}
            (id, history, head_id, item_id, name, position, checked)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
        ");
        let query = sqlx::query(&&query_text)
            .bind(tombstone.id.as_bytes().as_slice())
            .bind(Into::<Box<[u8]>>::into(tombstone.history))
            .bind(tombstone.head_id.as_bytes().as_slice())
            .bind(tombstone.item_id.as_bytes().as_slice())
            .bind(tombstone.name)
            .bind(tombstone.position.as_bytes())
            .bind(tombstone.checked);

        let _ = execute!(self, query, execute)
            .or_raise(|| StorageError::backend_write(
                format!("failed save tombstone to '{ITEM_TOMBSTONE_TABLE}'")
            ))?;

        Ok(())
    }

    fn load_item_operations(&mut self) -> Result<Vec<item::Operation>, StorageError> {
        self.load_item_operations_from(ITEM_OPERATION_TABLE)
    }

    fn load_item_tombstones(&mut self) -> Result<Vec<item::Tombstone>, StorageError> {
        let query_text = format!("
            SELECT id, history, head_id, item_id, name, position, checked
            FROM {ITEM_TOMBSTONE_TABLE}
        ");

        execute!(self, sqlx::query(&query_text), fetch_all)
            .or_raise(|| StorageError::backend_read(
                format!("failed load tombstones from '{ITEM_TOMBSTONE_TABLE}'"))
            )?
            .into_iter()
            .map(|row| Self::sqlite_row_to_item_tombstone(row))
            .collect()
    }

    fn load_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<item::Operation>, StorageError> {
        self.load_associated_item_operations_from(ITEM_OPERATION_TABLE, item_id)
    }

    fn load_associated_item_tombstone(&mut self, item_id: &Uuid) -> Result<Option<item::Tombstone>, StorageError> {
        let query_text = format!("
            SELECT id, history, head_id, item_id, name, position, checked
            FROM {ITEM_TOMBSTONE_TABLE}
            WHERE item_id = ?
        ");
        let query = sqlx::query(&query_text)
            .bind(item_id.as_bytes().as_slice());

        let row = execute!(self, query, fetch_optional)
            .or_raise(|| StorageError::backend_read(
                format!("failed load tombstone from '{ITEM_TOMBSTONE_TABLE}'"))
            )?;

        let tombstone = if let Some(row) = row {
            Some(Self::sqlite_row_to_item_tombstone(row)?)
        } else {
            None
        };

        Ok(tombstone)
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<item::Operation, StorageError> {
        let row = match self.erase_operation_from(ITEM_OPERATION_TABLE, id) {
            Ok(r) => r,
            Err(e) if e.kind == ErrorKind::BackendDelete => {
                self.erase_operation_from(ITEM_TOMBSTONE_TABLE, id)?
            },
            Err(e) => Err(e)?,
        };

        Self::sqlite_row_to_item_operation(row)
    }

    fn erase_item_tombstone(&mut self, id: &Uuid) -> Result<item::Tombstone, StorageError> {
        let query_text = format!("
            DELETE FROM {ITEM_TOMBSTONE_TABLE}
            WHERE ID = ?
            RETURNING id, history, head_id, item_id, name, position, checked
        ");
        let query = sqlx::query(&query_text)
            .bind(id.as_bytes().as_slice());

        let row = execute!(self, query, fetch_optional)
            .or_raise(|| StorageError::backend_delete(
                format!("failed to delete tombstone from {ITEM_TOMBSTONE_TABLE}"))
            )?
            .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))?;

        Ok(Self::sqlite_row_to_item_tombstone(row)?)
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
        let mut sqlite_pool = executor
            .block_on(SqlitePool::connect_with(sql_connection_options))
            .unwrap();
        let mut storage = SqliteStorage::new(&mut sqlite_pool, &executor).unwrap();

        storage.save_stamp(&Stamp::seed()).unwrap();
        let _ = storage.load_stamp().unwrap();
    }
}
