use super::*;
use crate::crossdev;
use filesize::PathExt;
use std::os::unix::prelude::MetadataExt;
use std::path::{Path, PathBuf};
use std::io;
use ::jwalk;

struct FSMetadata {
    metadata: std::fs::Metadata
}

impl Metadata for FSMetadata {
    fn is_dir(&self) -> bool {
        self.metadata.is_dir()
    }

    fn dev(&self) -> u64 {
        self.metadata.dev()
    }

    fn ino(&self) -> u64 {
        self.metadata.ino()
    }

    fn nlink(&self) -> u64 {
        self.metadata.nlink()
    }

    fn apparent_size(&self) -> u64 {
        self.metadata.len()
    }

    #[cfg(not(windows))]
    fn size_on_disk(&self, _parent: &Path, name: &Path) -> io::Result<u64> {
        name.size_on_disk_fast(&self.metadata)
    }
    
    #[cfg(windows)]
    fn size_on_disk(&self, parent: &Path, name: &Path) -> io::Result<u64> {
        parent.join(name).size_on_disk_fast(&self.metadata)
    }

    fn modified(&self) -> io::Result<SystemTime> {
        self.metadata.modified()
    }
}

pub struct JWalkWalkerDir {
    entry: jwalk::DirEntry<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>
}

impl WalkerDir for JWalkWalkerDir {
    fn depth(&self) -> usize {
        self.entry.depth
    }

    fn path(&self) -> PathBuf {
        self.entry.path()
    }

    fn file_name(&self) -> PathBuf {
        self.entry.file_name.clone().into()
    }

    fn parent_path(&self) -> &Path {
        self.entry.parent_path()
    }

    fn metadata(&self) -> Option<Result<Box<dyn Metadata>, io::Error>> {
        match &self.entry.client_state {
            Some(metadata) => {
                Some(match metadata {
                        Ok(metadata) => {
                            Ok(Box::new(FSMetadata { metadata: metadata.clone() }))
                        },
                        Err(err) => {
                            Err(io::Error::from_raw_os_error(0))
                        }
                })
            },
            _ => {
                None
            }
        }
    }
}

pub struct JWalkWalker {}

impl Walker for JWalkWalker {
    fn into_iter(&self, path: &Path, root_device_id: u64, options: WalkOptions) -> Box<dyn Iterator<Item = Result<Box<dyn WalkerDir>, io::Error>>> {
        Box::new(JWalkWalkerIterator::new(path, root_device_id, options))
    }
}

pub struct JWalkWalkerIterator {
    options: WalkOptions,
    walk_dir_iter: WalkDirIter,
}

impl JWalkWalkerIterator {
    fn new(root: &Path, root_device_id: u64, options: WalkOptions) -> JWalkWalkerIterator {
        let walk_dir_iter = WalkDir::new(root)
            .follow_links(false)
            .sort(match options.sorting {
                TraversalSorting::None => false,
                TraversalSorting::AlphabeticalByFileName => true,
            })
            .skip_hidden(false)
            .process_read_dir({
                let ignore_dirs = options.ignore_dirs.clone();
                let cross_filesystems = options.cross_filesystems;
                move |_, _, _, dir_entry_results| {
                    dir_entry_results.iter_mut().for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            let metadata = dir_entry.metadata();

                            if dir_entry.file_type.is_dir() {
                                let ok_for_fs = cross_filesystems
                                    || metadata
                                        .as_ref()
                                        .map(|m| crossdev::is_same_device(root_device_id, m))
                                        .unwrap_or(true);
                                if !ok_for_fs || ignore_dirs.contains(&dir_entry.path()) {
                                    dir_entry.read_children_path = None;
                                }
                            }

                            dir_entry.client_state = Some(metadata);
                        }
                    })
                }
            })
            .parallelism(match options.threads {
                0 => jwalk::Parallelism::RayonDefaultPool {
                    busy_timeout: std::time::Duration::from_secs(1),
                },
                1 => jwalk::Parallelism::Serial,
                _ => jwalk::Parallelism::RayonExistingPool {
                    pool: jwalk::rayon::ThreadPoolBuilder::new()
                        .stack_size(128 * 1024)
                        .num_threads(options.threads)
                        .thread_name(|idx| format!("dua-fs-walk-{idx}"))
                        .build()
                        .expect("fields we set cannot fail")
                        .into(),
                    busy_timeout: None,
                },
            }).into_iter();
        JWalkWalkerIterator {
            options,
            walk_dir_iter
        }
    }
}

impl Iterator for JWalkWalkerIterator {
    type Item = Result<Box<dyn WalkerDir>, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.walk_dir_iter.next()?;
        match entry {
            Ok(entry) => {
                Some(Ok(Box::new(JWalkWalkerDir{
                    entry
                })))
            },
            Err(err) => {
                Some(Err(err.into()))
            }
        }
    }
}

type WalkDir = jwalk::WalkDirGeneric<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>;
type WalkDirIter = jwalk::DirEntryIter<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>;
