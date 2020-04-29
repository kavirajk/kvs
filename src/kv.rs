use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{rename, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::result;

pub const DEFAULT_LOG_NAME: &'static str = "kv.log";

pub struct KvStore {
    pub index: HashMap<String, usize>,
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

        let mut kv = KvStore {
            index: HashMap::new(),
            log: p,
        };

        kv.fill_index()?;
        Ok(kv)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let f = OpenOptions::new().read(true).open(&self.log)?;
        if f.metadata()?.len() > (1 << 10) * 30 {
            self.compact()?;
        }

        let cmd = KvCommand::Set(key.clone(), val);

        let mut f = OpenOptions::new().append(true).open(&self.log)?;

        let offset = f.seek(SeekFrom::End(0))?;

        serde_json::to_writer(&mut f, &cmd)?;

        self.index.insert(key, offset as usize);

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, k: String) -> Result<Option<String>> {
        let entry = self.index.get(&k);

        let mut offset: usize = 0;

        match entry {
            None => return Ok(None),
            Some(v) => {
                offset = *v;
            }
        }

        let mut f = OpenOptions::new().read(true).open(&self.log)?;

        f.seek(SeekFrom::Start(offset as u64));

        let de = serde_json::Deserializer::from_reader(&mut f);

        let mut stream = de.into_iter::<KvCommand>();

        match stream.next() {
            None => {
                return Err(KvError {
                    msg: format!("value not found in the offset: {}", offset),
                })
            }
            Some(cmd) => {
                let cmd = cmd?;
                if let KvCommand::Set(_, v) = cmd {
                    return Ok(Some(v));
                }
            }
        }

        return Err(KvError {
            msg: format!("remove command at the offset: {}", offset),
        });
    }

    /// Remove a given key.
    pub fn remove(&mut self, k: String) -> Result<()> {
        let entry = self.index.get(&k);

        match entry {
            None => {
                return Err(KvError {
                    msg: "Key not found".to_owned(),
                })
            }
            Some(_) => {}
        }

        let cmd = KvCommand::Remove(k.clone());

        let mut f = OpenOptions::new().append(true).open(&self.log)?;

        serde_json::to_writer(&mut f, &cmd)?;

        self.index.remove(&k);

        Ok(())
    }

    fn fill_index(&mut self) -> Result<()> {
        let mut f = OpenOptions::new().read(true).open(&self.log)?;

        let de = serde_json::Deserializer::from_reader(&mut f);

        let mut stream = de.into_iter::<KvCommand>();

        let mut offset = stream.byte_offset();

        loop {
            match stream.next() {
                Some(cmd) => {
                    let cmd = cmd?;
                    match cmd {
                        KvCommand::Set(k, _) => {
                            self.index.insert(k, offset);
                            offset = stream.byte_offset();
                        }
                        KvCommand::Remove(k) => {
                            self.index.remove(&k);
                        }
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let tmp = &self.log.parent().unwrap(); // shouldn't panic!

        let tmp = tmp.join("kvs.comp");

        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&tmp)?;

        let mut tmap: HashMap<String, usize> = HashMap::new();

        let mut currf = OpenOptions::new().read(true).open(&self.log)?;

        let mut new_offset: u64 = 0;

        for (k, offset) in self.index.iter() {
            currf.seek(SeekFrom::Start(*offset as u64))?;
            let de = serde_json::Deserializer::from_reader(&currf);
            let mut stream = de.into_iter::<KvCommand>();
            let cmd = stream.next().unwrap().unwrap(); // shouldn't panic

            serde_json::to_writer(&mut f, &cmd)?;
            tmap.insert(k.to_owned(), new_offset as usize);

            new_offset = f.seek(SeekFrom::End(0))?;
        }

        // atomic steps
        rename(&tmp, &self.log)?;
        self.index = tmap;

        Ok(())
    }
}
