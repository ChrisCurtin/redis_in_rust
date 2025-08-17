// List of all the keys, and their types

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::index::KeyType::Undefined;

#[derive(Debug, Clone)]
pub struct Index {
    shared: Arc<InternalStorage>
}

impl Index {
    pub fn new() -> Index {
        Index {
            shared: Arc::new(InternalStorage::new())
        }
    }
    pub fn add(&self, key: String) {
        self.shared.entries.lock().unwrap().insert(key, KeyType::String);
    }
    pub fn get(&self, key: String) -> KeyType {
        let value = self.shared.entries.lock().unwrap().get(&key);
        match value {
            Some(v) => return *v.unwrap().clone(),
            None => Undefined
        }
        
    }
}

#[derive(Debug)]
enum KeyType {
    Undefined,
    String,
    Integer,
    List
}

#[derive(Debug)]
struct InternalStorage {
    entries: Mutex<HashMap<String, KeyType>>
}

impl InternalStorage {
    fn new() -> InternalStorage {
        InternalStorage {
            entries: Mutex::new(HashMap::new())
        }
    }
}