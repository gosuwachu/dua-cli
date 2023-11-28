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
    use super::*;
    use crate::fs_walk::mocks::*;
    use std::collections::HashMap;
    use std::time::SystemTime;

    #[test]
    fn test_aggregate() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "test/a.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(10), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "test/b.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(20), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);
        entries.insert("other".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "other/a.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(7), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "other/b.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(5), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            false,
            vec!["test", "other"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 2);
        assert_eq!(res.total, 42);
        assert_eq!(stats.entries_traversed, 4);
        assert_eq!(stats.largest_file_in_bytes, 20);
        assert_eq!(stats.smallest_file_in_bytes, 5);
        assert_eq!(list.len(), 2);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 30u128, 0u64),
            (PathBuf::from("other"), 12u128, 0u64),
        ]);
    }

    #[test]
    fn test_aggregate_size_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "test/a.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Err(io::Error::from(io::ErrorKind::Other)), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);
        
        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            false,
            vec!["test"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 1);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(stats.largest_file_in_bytes, 0);
        assert_eq!(stats.smallest_file_in_bytes, 0);
        assert_eq!(list.len(), 1);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 0u128, 1u64),
        ]);
    }

    #[test]
    fn test_aggregate_empty() {
        let entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        
        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            false,
            vec!["test"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 0);
        assert_eq!(stats.largest_file_in_bytes, 0);
        assert_eq!(stats.smallest_file_in_bytes, 0);
        assert_eq!(list.len(), 1);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 0u128, 0u64)
        ]);
    }

    #[test]
    fn test_aggregate_cross_filesystem() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "test/a.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(10), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "test/b.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(20), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);
        entries.insert("other".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "other/a.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(7), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "other/b.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(5), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);

        let walker = MockWalker { device_id: Ok(1), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            false,
            vec!["test", "other"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 2);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 4);
        assert_eq!(stats.largest_file_in_bytes, 0);
        assert_eq!(stats.smallest_file_in_bytes, 0);
        assert_eq!(list.len(), 2);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 0u128, 0u64),
            (PathBuf::from("other"), 0u128, 0u64),
        ]);
    }

    #[test]
    fn test_aggregate_apparent_size() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "test/a.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 1, 
                    size_on_disk: Ok(10), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "test/b.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 2, 
                    size_on_disk: Ok(20), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);
        entries.insert("other".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "other/a.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 3, 
                    size_on_disk: Ok(7), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "other/b.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 4, 
                    size_on_disk: Ok(5), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions{apparent_size: true, ..WalkOptions::default()};

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            false,
            vec!["test", "other"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 2);
        assert_eq!(res.total, 10);
        assert_eq!(stats.entries_traversed, 4);
        assert_eq!(stats.largest_file_in_bytes, 4);
        assert_eq!(stats.smallest_file_in_bytes, 1);
        assert_eq!(list.len(), 2);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 3u128, 0u64),
            (PathBuf::from("other"), 7u128, 0u64),
        ]);
    }

    #[test]
    fn test_aggregate_sort_by_bytes() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "test/a.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(10), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "test/b.txt".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(20), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);
        entries.insert("other".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "a.txt".into(),
                path: "other/a.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(7), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            }),
            Ok(MockEntry{
                dept: 0,
                file_name: "b.txt".into(),
                path: "other/b.txt".into(),
                parent_path: "other".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    dev: 0, 
                    ino: 0, 
                    nlink: 0, 
                    apparent_size: 0, 
                    size_on_disk: Ok(50), 
                    modified: Ok(SystemTime::UNIX_EPOCH) }))
            })
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            vec!["test", "other"].into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 2);
        assert_eq!(res.total, 87);
        assert_eq!(stats.entries_traversed, 4);
        assert_eq!(stats.largest_file_in_bytes, 50);
        assert_eq!(stats.smallest_file_in_bytes, 7);
        assert_eq!(list.len(), 2);
        assert_eq!(list, vec![
            (PathBuf::from("test"), 30u128, 0u64),
            (PathBuf::from("other"), 57u128, 0u64),
        ]);
    }

    #[test]
    fn test_aggregate_directory_ignored() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Ok(MockEntry{
            dept: 0,
            file_name: "a".into(),
            path: "test/a".into(),
            parent_path: "test".into(),
            metadata: Some(Ok(MockMetadata { 
                is_dir: true, 
                dev: 0, 
                ino: 0, 
                nlink: 0, 
                apparent_size: 11, 
                size_on_disk: Ok(10), 
                modified: Ok(SystemTime::UNIX_EPOCH) }))
        })]);

        let walker = MockWalker { device_id: Ok(0), entries };
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
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_aggregate_entry_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Err(io::Error::from(io::ErrorKind::Other))]);

        let paths = entries.keys().map(|p| match p.to_str() {
            Some(str) => str.to_string(),
            _ => "".to_string()
        }).collect::<Vec<String>>();

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            paths.into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 1);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_aggregate_device_id_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Ok(MockEntry{
            dept: 0,
            file_name: "a".into(),
            path: "test/a".into(),
            parent_path: "test".into(),
            metadata: Some(Ok(MockMetadata { 
                is_dir: true, 
                dev: 0, 
                ino: 0, 
                nlink: 0, 
                apparent_size: 11, 
                size_on_disk: Ok(10), 
                modified: Ok(SystemTime::UNIX_EPOCH) }))
        })]);

        let paths = entries.keys().map(|p| match p.to_str() {
            Some(str) => str.to_string(),
            _ => "".to_string()
        }).collect::<Vec<String>>();

        let walker = MockWalker { device_id: Err(io::Error::from(io::ErrorKind::Other)), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            paths.into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 1);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 0);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_aggregate_metadata_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Ok(MockEntry{
            dept: 0,
            file_name: "a".into(),
            path: "test/a".into(),
            parent_path: "test".into(),
            metadata: Some(Err(io::Error::from(io::ErrorKind::Other)))
        })]);

        let paths = entries.keys().map(|p| match p.to_str() {
            Some(str) => str.to_string(),
            _ => "".to_string()
        }).collect::<Vec<String>>();

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            paths.into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 1);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_aggregate_metadata_none() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![Ok(MockEntry{
            dept: 0,
            file_name: "a".into(),
            path: "test/a".into(),
            parent_path: "test".into(),
            metadata: None
        })]);

        let paths = entries.keys().map(|p| match p.to_str() {
            Some(str) => str.to_string(),
            _ => "".to_string()
        }).collect::<Vec<String>>();

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let result = aggregate(
            Option::<io::Stderr>::None,
            walker,
            &walk_options,
            true,
            paths.into_iter(),
        );

        let (res, stats, list) = result.unwrap();
        assert_eq!(res.num_errors, 0);
        assert_eq!(res.num_roots, 1);
        assert_eq!(res.total, 0);
        assert_eq!(stats.entries_traversed, 1);
        assert_eq!(list.len(), 1);
    }
}
