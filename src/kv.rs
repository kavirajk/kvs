use std::collections::HashMap;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Creates a `KvStore`
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, k: String, v: String) {
        self.store.insert(k, v);
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, k: String) -> Option<String> {
        self.store.get(&k).cloned()
    }

    /// Remove a given key.
    pub fn rm(&mut self, k: String) {
        self.store.remove(&k).unwrap();
    }
}
