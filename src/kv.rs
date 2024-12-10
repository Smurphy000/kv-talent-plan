use serde::{Deserialize, Serialize};

use crate::DataStoreError;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};
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
    map: HashMap<String, String>, // This will be the index
    wal: WAL,                     // WAL
}

#[derive(Default)]
struct WAL {
    /// Line limit for log file before compaction should occur
    threshold: u128, // currently this is number of lines, but should rather by size on disk
    handle: Option<File>, // opened file handle
}

impl WAL {
    fn new(p: &Path) -> Result<Self, DataStoreError> {
        Ok(Self {
            threshold: 100,
            handle: Some(OpenOptions::new().write(true).append(true).open(p)?),
        })
    }

    // append some serialized data to the log
    fn append(&mut self, data: String) -> Result<(), DataStoreError> {
        self.handle.as_mut().unwrap().write(data.as_bytes())?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Commands {
    Set(String, String),
    Rm(String),
    Get(String),
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn new(p: &Path) -> Result<KvStore, DataStoreError> {
        Ok(KvStore {
            map: HashMap::new(),
            wal: WAL::new(p)?,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<(), DataStoreError> {
        //! this may be an extra clone
        let v = serde_json::to_string(&Commands::Set(key.clone(), value.clone()))?;
        let _ = self.wal.append(v);
        // after command is persisted, we update the in-mem index
        self.map.insert(key, value);

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>, DataStoreError> {
        // Search map first, if not present
        Ok(self.map.get(&key).cloned())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<(), DataStoreError> {
        if !self.map.contains_key(&key) {
            return Err(DataStoreError::KeyNotFound);
        }
        let v = serde_json::to_string(&Commands::Rm(key.clone()))?;
        let _ = self.wal.append(v);
        self.map.remove(&key);

        Ok(())
    }

    /// Compact the log file when it exceeds a certain size threshold
    fn compact() {
        unimplemented!()
    }

    /// Initializes the in-mem index by regenerating from the existing log
    fn intialize_index(&mut self, path: &Path) -> Result<(), DataStoreError> {
        let f = File::open(path)?;

        // Collect all data from logs to generate the in memory index
        let data: Vec<Commands> = serde_json::Deserializer::from_reader(f)
            .into_iter()
            .map(|f| f.unwrap())
            .collect::<Vec<Commands>>();

        //? Could not figure out a functional way to do this
        let mut map: HashMap<String, String> = HashMap::new();
        for d in data {
            match d {
                Commands::Set(k, v) => {
                    map.insert(k, v);
                    ()
                }
                Commands::Rm(k) => {
                    map.remove(&k);
                    ()
                }
                Commands::Get(_) => (),
            }
        }
        self.map = map;
        Ok(())
    }

    /// Open and intialize in-mem index from provided log file
    pub fn open(path: &Path) -> Result<KvStore, DataStoreError> {
        let f = path.join("log.txt");
        if !f.exists() {
            File::create(&f)?;
        }

        let mut store = KvStore::new(&f)?;
        store.intialize_index(&f)?;
        Ok(store)
    }
}
