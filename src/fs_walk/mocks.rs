use crate::fs_walk::{Entry, Metadata, Walker};

use std::{io, path::Path};
use std::collections::HashMap;
use std::result::Result;
use std::time::SystemTime;
use std::vec;
use std::path::PathBuf;

pub struct MockMetadata {
    pub is_dir: bool,
    pub dev: u64,
    pub ino: u64,
    pub nlink: u64,
    pub apparent_size: u64,
    pub size_on_disk: io::Result<u64>,
    pub modified: io::Result<std::time::SystemTime>,
}

impl Clone for MockMetadata {
    fn clone(&self) -> Self {
        Self {
            is_dir: self.is_dir.clone(),
            dev: self.dev.clone(),
            ino: self.ino.clone(),
            nlink: self.nlink.clone(),
            apparent_size: self.apparent_size.clone(),
            size_on_disk: match &self.size_on_disk {
                Ok(size) => Ok(*size),
                Err(err) => Err(io::Error::from(err.kind())),
            },
            modified: match &self.modified {
                Ok(time) => Ok(time.to_owned()),
                Err(err) => Err(io::Error::from(err.kind())),
            },
        }
    }
}

impl Default for MockMetadata {
    fn default() -> Self {
        Self {
            is_dir: false,
            dev: 0,
            ino: 0,
            nlink: 0,
            apparent_size: 0,
            size_on_disk: Ok(0),
            modified: Ok(SystemTime::UNIX_EPOCH),
        }
    }
}

impl Metadata for MockMetadata {
    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn dev(&self) -> u64 {
        self.dev
    }

    fn ino(&self) -> u64 {
        self.ino
    }

    fn nlink(&self) -> u64 {
        self.nlink
    }

    fn apparent_size(&self) -> u64 {
        self.apparent_size
    }

    fn size_on_disk(&self) -> io::Result<u64> {
        match &self.size_on_disk {
            Ok(size) => Ok(*size),
            Err(err) => Err(io::Error::from(err.kind())),
        }
    }

    fn modified(&self) -> io::Result<std::time::SystemTime> {
        match &self.modified {
            Ok(time) => Ok(time.to_owned()),
            Err(err) => Err(io::Error::from(err.kind())),
        }
    }
}

pub struct MockEntry {
    pub dept: usize,
    pub path: PathBuf,
    pub file_name: PathBuf,
    pub parent_path: PathBuf,
    pub metadata: Option<Result<MockMetadata, io::Error>>,
}

impl Default for MockEntry {
    fn default() -> Self {
        Self {
            dept: Default::default(),
            path: Default::default(),
            file_name: Default::default(),
            parent_path: Default::default(),
            metadata: None,
        }
    }
}

impl Clone for MockEntry {
    fn clone(&self) -> Self {
        Self {
            dept: self.dept.clone(),
            path: self.path.clone(),
            file_name: self.file_name.clone(),
            parent_path: self.parent_path.clone(),
            metadata: match &self.metadata {
                Some(Ok(metadata)) => Some(Ok(metadata.clone())),
                Some(Err(err)) => Some(Err(io::Error::from(err.kind()))),
                _ => None,
            },
        }
    }
}

impl Entry for MockEntry {
    fn depth(&self) -> usize {
        self.dept
    }

    fn path(&self) -> PathBuf {
        self.path.clone()
    }

    fn file_name(&self) -> PathBuf {
        self.file_name.clone()
    }

    fn parent_path(&self) -> PathBuf {
        self.parent_path.clone()
    }

    fn metadata(&self) -> Option<Result<impl Metadata + '_, io::Error>> {
        match &self.metadata {
            Some(Ok(metadata)) => Some(Ok(metadata.clone())),
            Some(Err(err)) => Some(Err(io::Error::from(err.kind()))),
            _ => None,
        }
    }
}

pub struct MockWalker {
    pub device_id: io::Result<u64>,
    pub entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>>,
}

impl Walker for MockWalker {
    fn device_id(&self, _path: &Path) -> io::Result<u64> {
        match &self.device_id {
            Ok(dev) => Ok(*dev),
            Err(err) => Err(io::Error::from(err.kind()))
        }
    }

    fn into_iter(
        &mut self,
        path: &Path,
        _root_device_id: u64,
    ) -> impl Iterator<Item = Result<impl Entry, io::Error>> {
        let mut empty: Vec<Result<MockEntry, io::Error>> = Vec::new();
        let path_entries = self.entries.get_mut(path).unwrap_or(&mut empty);

        MockIterator {
            iter: std::mem::replace(path_entries, Vec::new()).into_iter(),
        }
    }
}

pub struct MockIterator {
    iter: vec::IntoIter<Result<MockEntry, io::Error>>,
}

impl Iterator for MockIterator {
    type Item = Result<MockEntry, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(entry) => match &entry {
                Ok(entry) => Some(Ok(entry.clone())),
                Err(err) => Some(Err(io::Error::from(err.kind()))),
            },
            None => None,
        }
    }
}