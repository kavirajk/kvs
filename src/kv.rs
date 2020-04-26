use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;
use std::thread;
use std::time;

pub const DEFAULT_LOG_NAME: &'static str = "kv.log";

pub struct KvStore {
    index: HashMap<String, String>,
    log: PathBuf,
}

#[derive(Debug)]
pub struct KvError {
    msg: String,
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)?;
        Ok(())
    }
}

pub type Result<T> = result::Result<T, KvError>;

impl Error for KvError {
    fn description(&self) -> &str {
        &self.msg
    }
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError {
            msg: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError {
            msg: err.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KvCommand {
    Set(String, String),
    Remove(String),
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut p = path.into();

        if p.is_dir() {
            p = p.join(DEFAULT_LOG_NAME);
        }

        // make sure to create the log file.
        // so that 'get' or 'set' can assume that file already exists.
        OpenOptions::new().write(true).create(true).open(&p)?;

        let kv = KvStore {
            index: HashMap::new(),
            log: p,
        };
        Ok(kv)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = KvCommand::Set(key, val);

        let mut f = OpenOptions::new().append(true).open(&self.log)?;

        serde_json::to_writer(&mut f, &cmd)?;

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, k: String) -> Result<Option<String>> {
        let mut f = OpenOptions::new().read(true).open(&self.log)?;

        let de = serde_json::Deserializer::from_reader(&mut f);

        let mut stream = de.into_iter::<KvCommand>();

        let mut map = HashMap::new();

        loop {
            match stream.next() {
                Some(cmd) => {
                    let cmd = cmd?;
                    match cmd {
                        KvCommand::Set(k, v) => {
                            map.insert(k, v);
                        }
                        KvCommand::Remove(k) => {
                            map.remove(&k);
                        }
                    }
                }
                None => break,
            }
        }

        if !map.contains_key(&k) {
            return Ok(None);
        }

        Ok(map.get(&k).and_then(|x| Some(x.clone())))
    }

    /// Remove a given key.
    pub fn remove(&mut self, k: String) -> Result<()> {
        let mut f = OpenOptions::new().read(true).open(&self.log)?;

        let de = serde_json::Deserializer::from_reader(&mut f);

        let mut stream = de.into_iter::<KvCommand>();

        let mut map = HashMap::new();

        loop {
            match stream.next() {
                Some(cmd) => {
                    let cmd = cmd?;
                    match cmd {
                        KvCommand::Set(k, v) => {
                            map.insert(k, v);
                        }
                        KvCommand::Remove(k) => {
                            map.remove(&k);
                        }
                    }
                }
                None => break,
            }
        }

        if !map.contains_key(&k) {
            return Err(KvError {
                msg: "Key not found".to_owned(),
            });
        };

        let cmd = KvCommand::Remove(k.clone());

        let mut f = OpenOptions::new().append(true).open(&self.log)?;

        serde_json::to_writer(&mut f, &cmd)?;
        Ok(())
    }
}
