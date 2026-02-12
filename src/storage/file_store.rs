use std::fs::{File, OpenOptions};
use std::io::Write;

use exn::{Result, ResultExt};
use itc::Stamp;

use super::model::ChecklistHeadEvent;
use super::storage_error::StorageError;
use super::store::Store;


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

impl Store for FileStore {
    fn save_stamp(&mut self, stamp: &Stamp) -> Result<(), StorageError> {
        todo!()
    }
    
    fn load_stamp(&mut self) -> Result<Stamp, StorageError> {
        todo!()
    }

    fn save_head(&mut self, event: &ChecklistHeadEvent) -> Result<(), StorageError> {
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

    fn load_all_heads(&self) -> Result<Vec<ChecklistHeadEvent>, StorageError> {
        todo!()
    }

    fn delete_head(&mut self, head_id: &uuid::Uuid) -> Result<bool, StorageError> {
        todo!()
    }

    fn save_item(&mut self, event: &super::model::ChecklistItemEvent) -> Result<(), StorageError> {
        todo!()
    }

    fn load_all_items(&self) -> Result<Vec<super::model::ChecklistItemEvent>, StorageError> {
        todo!()
    }

    fn delete_item(&mut self, item_id: &uuid::Uuid) -> Result<bool, StorageError> {
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

        file_store.save_head(&head).unwrap();
    }
}