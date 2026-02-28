use core::future::Future;
use exn::{OptionExt, Result, ResultExt, bail};
use itc::{EventTree, Stamp};
use loro_fractional_index::FractionalIndex;
use sqlx::pool::PoolConnection;
use sqlx::{Connection, Row, Sqlite, Transaction};
use sqlx::sqlite::SqliteRow;
use uuid::Uuid;

use crate::persistence::model::checklist::{HeadOperation, ItemOperation};
use crate::persistence::StorageError;
use crate::persistence::storage::Store;


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

        let mut length_as_bytes = Self::usize_to_vec_u8(length);
        length_as_bytes.extend(string_as_bytes);

        length_as_bytes
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
            _ => bail!(StorageError::data_decode("encountered unknown item operation kind"))
        }
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
        let query = sqlx::query(
                r#"
                    INSERT INTO head_operation
                    (id, history, secondary_id, variant, payload)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO NOTHING
                "#
            )
            .bind(operation.id().as_bytes().as_slice())
            .bind(Into::<Box<[u8]>>::into(operation.history()))
            .bind(operation.head_id().as_bytes().as_slice())
            .bind(Self::head_to_variant_id(&operation))
            .bind(Self::head_to_payload(&operation));

        let _ = self.executor.block_on(
                query
                    .execute(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_write("failed save head operation"))?;

        Ok(())
    }

    fn load_all_head_operations(&mut self) -> Result<Vec<HeadOperation>, StorageError> {
        let query = sqlx::query(
                r#"
                    SELECT id, history, secondary_id, variant, payload
                    FROM head_operation
                "#
            );

        let row = self.executor.block_on(
            query
                    .fetch_all(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_read("failed load head operations"))?;

        row.into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn load_all_associated_head_operations(&mut self, head_id: &Uuid) -> Result<Vec<HeadOperation>, StorageError> {
        let query = sqlx::query(
                r#"
                    SELECT id, history, secondary_id, variant, payload
                    FROM head_operation
                    WHERE secondary_id = ?
                "#
            )
            .bind(head_id.as_bytes().as_slice());

        let row = self.executor.block_on(
            query
                    .fetch_all(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_read("failed load head operations"))?;

        row.into_iter()
            .map(|row| Self::sqlite_row_to_head_operation(row))
            .collect()
    }

    fn erase_head_operation(&mut self, id: &Uuid) -> Result<HeadOperation, StorageError> {
        let query = sqlx::query(
                r#"
                    DELETE FROM head_operation
                    WHERE ID = ?
                    RETURNING id, history, secondary_id, variant, payload
                "#
            )
            .bind(id.as_bytes().as_slice());

        let row = self.executor.block_on(
                query
                    .fetch_optional(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to delete head operation"))?
            .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))?;

        Self::sqlite_row_to_head_operation(row)
    }

    fn save_item_operation(&mut self, operation: ItemOperation) -> Result<(), StorageError> {
        let query = sqlx::query(
                r#"
                    INSERT INTO head_operation
                    (id, history, secondary_id, variant, payload)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO NOTHING
                "#
            )
            .bind(operation.id().as_bytes().as_slice())
            .bind(Into::<Box<[u8]>>::into(operation.history()))
            .bind(operation.item_id().as_bytes().as_slice())
            .bind(Self::item_to_variant_id(&operation))
            .bind(Self::item_to_payload(&operation));

        let _ = self.executor.block_on(
                query
                    .execute(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_write("failed to save item operation"))?;

        Ok(())
    }

    fn load_all_item_operations(&mut self) -> Result<Vec<ItemOperation>, StorageError> {
        let query = sqlx::query(
                r#"
                   SELECT id, history, secondary_id, variant, payload
                   FROM item_operation
                "#
            );

        let row = self.executor.block_on(
            query
                    .fetch_all(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_read("expected record, found none"))?;

        row.into_iter()
            .map(|row| Self::sqlite_row_to_item_operation(row))
            .collect()
    }

    fn load_all_associated_item_operations(&mut self, item_id: &Uuid) -> Result<Vec<ItemOperation>, StorageError> {
        let query = sqlx::query(
                r#"
                    SELECT id, history, secondary_id, variant, payload
                    FROM item_operation
                    WHERE ID = ?
                "#
            )
            .bind(item_id.as_bytes().as_slice());

        let row = self.executor.block_on(
            query
                    .fetch_all(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_read("failed to load item operations"))?;

        row.into_iter()
            .map(|row| Self::sqlite_row_to_item_operation(row))
            .collect()
    }

    fn erase_item_operation(&mut self, id: &Uuid) -> Result<ItemOperation, StorageError> {
        let query = sqlx::query(
                r#"
                    DELETE FROM item_operation
                    WHERE ID = ?
                    RETURNING id, history, secondary_id, variant, payload
                "#
            )
            .bind(id.as_bytes().as_slice());

        let row = self.executor.block_on(
                query
                    .fetch_optional(&mut **self.sqlite_connection)
            )
            .or_raise(|| StorageError::backend_specific("failed to delete item operation"))?
            .ok_or_raise(|| StorageError::backend_specific("expected record, found none"))?;

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
