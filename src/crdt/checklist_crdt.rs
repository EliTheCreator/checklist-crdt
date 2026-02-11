use crate::storage::store::Store;
use crate::transport::transport::Transport;

pub struct ChecklistCrdt {
    storage: Box<dyn Store>,
    transport: Box<dyn Transport>,
}

impl ChecklistCrdt {
    pub fn new(storage: Box<dyn Store>, transport: Box<dyn Transport>) -> Self {
        ChecklistCrdt {
            storage: storage,
            transport: transport,
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn init_test() {
        use crate::storage::file_store::FileStore;

        struct DummyTransport {}
        impl Transport for DummyTransport {}

        let file_store = Box::new(FileStore::new("./test2.txt").unwrap());
        let transport = Box::new(DummyTransport {});

        let crdt = ChecklistCrdt::new(file_store, transport);
    }
}