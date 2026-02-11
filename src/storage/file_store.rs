use std::fs::{File, OpenOptions};
use std::io::Write;

use exn::{Result, ResultExt};

use super::model::ChecklistHeadEvent;
use super::storage_error::StorageError;
use super::store::{Store, StoreHead, StoreItem};


pub struct FileStore {
    file: File,
}

impl FileStore {
    pub fn new(path: &str)  -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .or_raise(|| StorageError(format!("failed to open file at {path}")))?;

        Ok(FileStore { file: file })
    }
}

impl Store for FileStore {}

impl StoreHead for FileStore {
    fn save(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError> {
        use ChecklistHeadEvent::*;
        let line = match event {
            Creation { meta, template_id, name, description } => {
                format!(
                    "Creation {} {} {} {} \"{}\" \"{}\"\n",
                    meta.id,
                    meta.head_id,
                    meta.itc_event,
                    template_id.map_or("".to_string(), |id| id.to_string()),
                    name,
                    description.clone().map_or("".to_string(), |s| s),
                )
            },
            NameUpdate { meta, name } => {
                format!("NameUpdate {}\n", meta.id)
            },
            DescriptionUpdate { meta, description } => {
                format!("DescriptionUpdate {}\n", meta.id)
            },
            CompletedUpdate { meta, completed } => {
                format!("CompletedUpdate {}\n", meta.id)
            },
            Deletion { meta } => {
                format!("Deletion {}\n", meta.id)
            },
        };

        self.file.write(line.as_bytes())
            .or_raise(|| StorageError(format!("failed to write line to file")))?;
        Ok(())
    }

    fn load(&self) -> Result<ChecklistHeadEvent, StorageError> {
        todo!()
    }
}


impl StoreItem for FileStore {
    fn save(&mut self, event: &super::model::ChecklistItemEvent) -> Result<(), StorageError> {
        todo!()
    }

    fn load(&self) -> Result<super::model::ChecklistItemEvent, StorageError> {
        todo!()
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_to_file() {
        let path = "./test.txt";
        let mut file_store = FileStore::new(path).unwrap();

        
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

        StoreHead::save(&mut file_store, &head).unwrap();
    }
}