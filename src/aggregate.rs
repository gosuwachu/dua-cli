use crate::fs_walk::{Entry, Metadata, WalkOptions, Walker};
use crate::{crossdev, InodeFilter, Throttle, WalkResult};
use anyhow::Result;
use std::path::PathBuf;
use std::time::Duration;
use std::{io, path::Path};

/// Aggregate the given `paths` and write information about them to `out` in a human-readable format.
/// If `compute_total` is set, it will write an additional line with the total size across all given `paths`.
/// If `sort_by_size_in_bytes` is set, we will sort all sizes (ascending) before outputting them.
pub fn aggregate(
    mut err: Option<impl io::Write>,
    mut walker: impl Walker,
    walk_options: &WalkOptions,
    sort_by_size_in_bytes: bool,
    paths: impl IntoIterator<Item = impl AsRef<Path>>,
) -> Result<(WalkResult, Statistics, Vec<(PathBuf, u128, u64)>)> {
    let mut res = WalkResult::default();
    let mut stats = Statistics {
        smallest_file_in_bytes: u128::max_value(),
        ..Default::default()
    };
    let mut aggregates = Vec::new();
    let mut inodes = InodeFilter::default();
    let progress = Throttle::new(Duration::from_millis(100), Duration::from_secs(1).into());

    for path in paths.into_iter() {
        res.num_roots += 1;
        let mut num_bytes = 0u128;
        let mut num_errors = 0u64;
        let device_id = match walker.device_id(path.as_ref()) {
            Ok(id) => id,
            Err(_) => {
                num_errors += 1;
                res.num_errors += 1;
                aggregates.push((path.as_ref().to_owned(), num_bytes, num_errors));
                continue;
            }
        };
        for entry in walker.into_iter(path.as_ref(), device_id) {
            stats.entries_traversed += 1;
            progress.throttled(|| {
                if let Some(err) = err.as_mut() {
                    write!(err, "Enumerating {} entries\r", stats.entries_traversed).ok();
                }
            });
            match entry {
                Ok(entry) => {
                    let file_size = match &entry.metadata() {
                        Some(Ok(ref m))
                            if !m.is_dir()
                                && (walk_options.count_hard_links
                                    || inodes.add_raw(m.dev(), m.ino(), m.nlink()))
                                && (walk_options.cross_filesystems
                                    || crossdev::is_same_device_raw(device_id, m.dev())) =>
                        {
                            if walk_options.apparent_size {
                                m.apparent_size()
                            } else {
                                m.size_on_disk().unwrap_or_else(|_| {
                                    num_errors += 1;
                                    0
                                })
                            }
                        }
                        Some(Ok(_)) => 0,
                        Some(Err(_)) => {
                            num_errors += 1;
                            0
                        }
                        None => 0, // ignore directory
                    } as u128;
                    stats.largest_file_in_bytes = stats.largest_file_in_bytes.max(file_size);
                    stats.smallest_file_in_bytes = stats.smallest_file_in_bytes.min(file_size);
                    num_bytes += file_size;
                }
                Err(_) => num_errors += 1,
            }
        }

        if let Some(err) = err.as_mut() {
            write!(err, "\x1b[2K\r").ok();
        }

        aggregates.push((path.as_ref().to_owned(), num_bytes, num_errors));

        res.total += num_bytes;
        res.num_errors += num_errors;
    }

    if stats.entries_traversed == 0 {
        stats.smallest_file_in_bytes = 0;
    }

    if sort_by_size_in_bytes {
        aggregates.sort_by_key(|&(_, num_bytes, _)| num_bytes);
    }

    Ok((res, stats, aggregates))
}

/// Statistics obtained during a filesystem walk
#[derive(Default, Debug)]
pub struct Statistics {
    /// The amount of entries we have seen during filesystem traversal
    pub entries_traversed: u64,
    /// The size of the smallest file encountered in bytes
    pub smallest_file_in_bytes: u128,
    /// The size of the largest file encountered in bytes
    pub largest_file_in_bytes: u128,
}

#[cfg(test)]
mod aggregate_tests {
    use crate::Entry;

    use super::*;
    use std::collections::HashMap;
    use std::mem;
    use std::result::Result;
    use std::time::SystemTime;
    use std::vec;
    use std::{io, path::PathBuf};

    struct MockMetadata {
        is_dir: bool,
        dev: u64,
        ino: u64,
        nlink: u64,
        apparent_size: u64,
        size_on_disk: io::Result<u64>,
        modified: io::Result<std::time::SystemTime>,
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

    struct MockEntry {
        dept: usize,
        path: PathBuf,
        file_name: PathBuf,
        parent_path: PathBuf,
        metadata: Option<Result<MockMetadata, io::Error>>,
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

    impl Entry for MockEntry {
        fn depth(&self) -> usize {
            0
        }

        fn path(&self) -> PathBuf {
            "/aaaa".into()
        }

        fn file_name(&self) -> PathBuf {
            "aaaa".into()
        }

        fn parent_path(&self) -> PathBuf {
            "parent".into()
        }

        fn metadata(&self) -> Option<Result<impl Metadata + '_, io::Error>> {
            Some(Ok(MockMetadata::default()))
        }
    }

    struct MockWalker {
        entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>>,
    }

    impl Walker for MockWalker {
        fn device_id(&self, _path: &Path) -> io::Result<u64> {
            Ok(0)
        }

        fn into_iter(
            &mut self,
            path: &Path,
            root_device_id: u64,
        ) -> impl Iterator<Item = Result<impl Entry, io::Error>> {
            let mut empty: Vec<Result<MockEntry, io::Error>> = Vec::new();
            let path_entries = self.entries.get_mut(path).unwrap_or(&mut empty);

            MockIterator {
                iter: std::mem::replace(path_entries, Vec::new()).into_iter(),
            }
        }
    }

    struct MockIterator {
        iter: vec::IntoIter<Result<MockEntry, io::Error>>,
    }

    impl Iterator for MockIterator {
        type Item = Result<MockEntry, io::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.iter.next() {
                Some(entry) => match &entry {
                    Ok(entry) => Some(Ok(MockEntry {
                        dept: entry.dept,
                        path: entry.path.clone(),
                        file_name: entry.file_name.clone(),
                        parent_path: entry.parent_path.clone(),
                        metadata: match &entry.metadata{
                            Some(Ok(metadata)) => Some(Ok(metadata.clone())),
                            Some(Err(err)) => Some(Err(io::Error::from(err.kind()))),
                            _ => None
                        },
                    })),
                    Err(err) => Some(Err(io::Error::new(err.kind(), ""))),
                },
                None => None,
            }
        }
    }

    #[test]
    fn test_aggregate() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Ok(MockEntry::default())]);

        let walker = MockWalker { entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            vec!["test"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }
}
