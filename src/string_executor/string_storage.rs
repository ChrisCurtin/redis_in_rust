// TODO
// add the expiration logic
// add support for getting the list of all keys (need the commands though)
// add support for NX and XX (only set if key does not exist or if key exists)

use std::collections::HashMap;
use bytes::Bytes;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct StringStorage {
    shared: Arc<InternalStorage>
}

impl StringStorage {
    pub fn new() -> StringStorage {
        let shared = Arc::new(InternalStorage::new());
        StringStorage {shared}
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.shared.get(key)
    }
    pub fn set(&self, key: &str, value: &Bytes) {
        self.shared.set(key, value);
    }
    pub fn del(&self, key: &str) {
        self.shared.del(key);
    }
}
#[derive( Debug)]
struct Entry {
    data: Bytes
}

#[derive( Debug)]
struct InternalStorage {
    entries: Mutex<HashMap<String, Entry>>
}

impl InternalStorage {
    fn new() -> InternalStorage {
        InternalStorage {
            entries: Mutex::new(HashMap::new())
        }
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let entries = self.entries.lock().unwrap();
        entries.get(key).map(|entry| entry.data.clone())
    }
    pub fn set(&self, key: &str, value: &Bytes) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(key.to_string(), Entry { data: value.clone() });
    }
    pub fn del(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap();
        entries.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::string_executor::string_storage::InternalStorage;

    #[test]
    fn given_empty_storage_when_get_then_return_none() {
        let storage = InternalStorage::new();
        let result = storage.get("key");
        assert_eq!(result, None);
    }

    #[test]
    fn given_empty_storage_when_set_then_can_get_value() {
        let storage = InternalStorage::new();
        let value = bytes::Bytes::from("value");
        storage.set("key", &value);
        let result = storage.get("key");
        match result {
            Some(v) => assert_eq!(v, value),
            None => panic!("Expected value")
        }
    }

    #[test]
    fn given_empty_storage_when_del_then_can_get_none() {
        let storage = InternalStorage::new();
        storage.del("key");
        let result = storage.get("key");
        assert_eq!(result, None);
    }

    #[test]
    fn given_storage_when_del_then_delete () {
        let storage = InternalStorage::new();
        let value = bytes::Bytes::from("value");
        storage.set("key", &value);
        storage.del("key");
        let result = storage.get("key");
        assert_eq!(result, None);
    }
}