use crate::fs_walk::{WalkOptions, Walker, Entry, Metadata};
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
    use std::time::SystemTime;
    use std::{io, path::PathBuf};
    use std::result::Result;
    use std::mem;
    use std::collections::HashMap;
    use std::vec;

    struct MockMetadata {

    }

    impl Metadata for MockMetadata {
        fn is_dir(&self) -> bool {
            false
        }

        fn dev(&self) -> u64 {
            0
        }

        fn ino(&self) -> u64 {
            0
        }

        fn nlink(&self) -> u64 {
            0
        }

        fn apparent_size(&self) -> u64 {
            0
        }

        fn size_on_disk(&self) -> io::Result<u64> {
            Ok(0)
        }

        fn modified(&self) -> io::Result<std::time::SystemTime> {
            Ok(SystemTime::now())
        }
    }

    #[derive(Clone)]
    struct MockEntry{
        
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
            Some(Ok(MockMetadata{}))
        }
    }

    struct MockWalker {
        entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>>
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
            let mut empty : Vec<Result<MockEntry, io::Error>> = Vec::new();
            let path_entries = 
                self.entries.get_mut(path).unwrap_or(&mut empty);

            MockIterator{
                iter: std::mem::replace(path_entries, Vec::new()).into_iter()
            }
        }
    }

    struct MockIterator {
        iter: vec::IntoIter<Result<MockEntry, io::Error>>
    }

    impl Iterator for MockIterator{
        type Item = Result<MockEntry, io::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.iter.next() {
                Some(entry) => {
                    match &entry {
                        Ok(entry) => {
                            Some(Ok(entry.clone()))
                        },
                        Err(err) => {
                            Some(Err(io::Error::new(err.kind(), "")))
                        },
                    }
                },
                None => None,
            } 
        }
    }

    #[test]
    fn test_aggregate() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{})
        ]);
        
        let walker = MockWalker{
            entries
        };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None, 
            walker, 
            &walk_options, 
            true, 
            vec!["test"].into_iter()
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }
}