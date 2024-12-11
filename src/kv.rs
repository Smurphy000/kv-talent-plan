use serde::{Deserialize, Serialize};
use serde_json::{de::IoRead, StreamDeserializer};

use crate::DataStoreError;
use std::{
    collections::HashMap,
    env,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, DataStoreError};
/// # use std::env;
/// # fn try_main() -> Result<(),DataStoreError>{
/// let dir = env::current_dir()?;
/// let mut store = KvStore::open(&dir)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct KvStore<'a> {
    map: HashMap<String, (usize, usize)>, // This will be the index
    wal: WAL<'a>,                         // WAL
    final_offset: usize,                  //EOF byte
}

#[derive(Debug)]
struct WAL<'a> {
    size: u128, // current size of WAL
    /// Line limit for log file before compaction should occur
    threshold: u128, // currently this is number of lines, but should rather by size on disk
    // handle: Option<File>, // opened file handle
    path: &'a Path,
    file: &'a str,
}

impl<'a> WAL<'a> {
    fn new(path: &'a Path, file: &'a str) -> Self {
        Self {
            size: 0,
            threshold: 100,
            path,
            file,
        }
    }

    // overwrite the existing log with an empty file
    fn clear(&self) -> Result<(), DataStoreError> {
        File::create(self.path.join(self.file))?;
        Ok(())
    }

    // Stream read the log into a vector of commands
    fn stream(&self) -> Result<Vec<Commands>, DataStoreError> {
        let f = File::open(self.path.join(self.file))?;
        let commands = serde_json::Deserializer::from_reader(&f)
            .into_iter::<Commands>()
            .map(|c| c.unwrap())
            .collect::<Vec<Commands>>();
        Ok(commands)
    }

    // Read one command based off its position in the log
    fn read_one(&self, offsets: (usize, usize)) -> Result<Commands, DataStoreError> {
        let mut handle = OpenOptions::new()
            .read(true)
            .open(self.path.join(self.file))?;

        let mut buf = vec![0; offsets.1 - offsets.0];
        handle.seek(SeekFrom::Start(offsets.0 as u64))?;
        handle.read_exact(&mut buf)?;

        let command: Commands = serde_json::from_slice(&buf).unwrap();

        Ok(command)
    }

    // append some serialized data to the log
    fn append(&mut self, data: String) -> Result<usize, DataStoreError> {
        let mut handle = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.path.join(self.file))?;
        let num_bytes = handle.write(data.as_bytes())?;
        self.size += 1;
        Ok(num_bytes)
    }

    // True if number of records in the log exceeds the threshold
    fn exceeds(&self) -> bool {
        self.size > self.threshold
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Commands {
    Set(String, String),
    Rm(String),
    Get(String),
}

impl<'a> KvStore<'a> {
    /// Creates a `KvStore`.
    pub fn new(p: &'a Path) -> Self {
        KvStore {
            map: HashMap::new(),
            wal: WAL::new(p, "log.txt"),
            final_offset: 0,
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<(), DataStoreError> {
        //! this may be an extra clone
        let v = serde_json::to_string(&Commands::Set(key.clone(), value.clone()))?;
        let num_bytes = self.wal.append(v)?;
        // after command is persisted, we update the in-mem index
        self.map
            .insert(key, (self.final_offset, self.final_offset + num_bytes));
        self.final_offset += num_bytes;
        if self.wal.exceeds() {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>, DataStoreError> {
        if let Some(offsets) = self.map.get(&key).cloned() {
            match self.wal.read_one(offsets)? {
                Commands::Set(_, v) => return Ok(Some(v)),
                Commands::Rm(_) => return Ok(None),
                Commands::Get(_) => return Ok(None),
            }
        }

        Ok(None)
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<(), DataStoreError> {
        if !self.map.contains_key(&key) {
            return Err(DataStoreError::KeyNotFound);
        }
        let v = serde_json::to_string(&Commands::Rm(key.clone()))?;
        let _ = self.wal.append(v);
        self.map.remove(&key);
        if self.wal.exceeds() {
            self.compact()?;
        }
        Ok(())
    }

    /// Compact the log file when it exceeds a certain size threshold
    fn compact(&mut self) -> Result<(), DataStoreError> {
        // take a stream of Commands from the wal, into a map
        // also keep an ordered vec of keys to rebuild the log.
        let mut mapping: HashMap<String, String> = HashMap::new();
        let commands = self.wal.stream()?;
        for c in commands {
            match c {
                Commands::Set(k, v) => {
                    mapping.insert(k, v);
                    ()
                }
                Commands::Rm(k) => {
                    mapping.remove(&k);
                    ()
                }
                Commands::Get(_) => (),
            }
        }
        // if error occurs here, could be bad
        self.wal.clear()?;

        for (k, v) in mapping.iter() {
            let v = serde_json::to_string(&Commands::Set(k.clone(), v.clone()))?;
            let _ = self.wal.append(v)?;
        }

        Ok(())
        // then overwrite the log, maybe using a temp + swap, or
        // or just straight up overwrite for now
    }

    /// Initializes the in-mem index by regenerating from the existing log
    fn intialize_index(&mut self, path: &Path) -> Result<(), DataStoreError> {
        let f = File::open(path)?;
        let mut map: HashMap<String, (usize, usize)> = HashMap::new();

        // Collect all data from logs to generate the in memory index
        let mut stream = serde_json::Deserializer::from_reader(&f).into_iter::<Commands>();

        let mut current_offset: usize = 0;
        let mut size = 0;
        let mut processing = true;
        while processing {
            if let Some(command) = stream.next() {
                let offset = stream.byte_offset();

                match command? {
                    Commands::Set(k, _) => {
                        map.insert(k, (current_offset, offset));
                        ()
                    }
                    Commands::Rm(k) => {
                        map.remove(&k);
                        ()
                    }
                    Commands::Get(_) => (),
                }
                current_offset = offset;
                size += 1;
            } else {
                processing = false;
            }
        }

        self.final_offset = current_offset;
        self.wal.size = size;
        self.map = map;
        Ok(())
    }

    /// Open and intialize in-mem index from provided log file
    pub fn open(path: &Path) -> Result<KvStore, DataStoreError> {
        let file_name = "log.txt";
        let f = path.join(file_name);
        if !f.exists() {
            File::create(&f)?;
        }

        let mut store = KvStore::new(path);
        store.intialize_index(&f)?;
        Ok(store)
    }
}
