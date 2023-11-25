use super::*;
use crate::crossdev;
use ::jwalk;
use filesize::PathExt;
use std::io;
use std::os::unix::prelude::MetadataExt;
use std::path::{Path, PathBuf};

struct JWalkMetadata<'a> {
    entry: &'a JWalkEntry,
    metadata: std::fs::Metadata,
}

impl<'a> Metadata for JWalkMetadata<'a> {
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
    fn size_on_disk(&self) -> io::Result<u64> {
        self.entry.file_name().size_on_disk_fast(&self.metadata)
    }

    #[cfg(windows)]
    fn size_on_disk(&self) -> io::Result<u64> {
        self.entry
            .parent_path()
            .join(self.entry.file_name())
            .size_on_disk_fast(&self.metadata)
    }

    fn modified(&self) -> io::Result<SystemTime> {
        self.metadata.modified()
    }
}

struct JWalkEntry {
    entry: jwalk::DirEntry<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>,
}

impl Entry for JWalkEntry {
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

    fn metadata(&self) -> Option<Result<Box<dyn Metadata + '_>, io::Error>> {
        match &self.entry.client_state {
            Some(metadata) => Some(match metadata {
                Ok(metadata) => Ok(Box::new(JWalkMetadata {
                    entry: &self,
                    metadata: metadata.clone(),
                })),
                Err(err) => Err(io::Error::new(
                    err.io_error()
                        .map(|err| err.kind())
                        .unwrap_or(io::ErrorKind::Other),
                    "",
                )),
            }),
            _ => None,
        }
    }
}

pub struct JWalkWalker {
    pub options: WalkOptions,
}

impl Walker for JWalkWalker {
    fn into_iter(
        &self,
        path: &Path,
        root_device_id: u64,
    ) -> Box<dyn Iterator<Item = Result<Box<dyn Entry>, io::Error>>> {
        Box::new(JWalkIterator::new(path, root_device_id, &self.options))
    }
}

pub struct JWalkIterator {
    walk_dir_iter: WalkDirIter,
}

type WalkDir = jwalk::WalkDirGeneric<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>;
type WalkDirIter = jwalk::DirEntryIter<((), Option<Result<std::fs::Metadata, jwalk::Error>>)>;

impl JWalkIterator {
    fn new(root: &Path, root_device_id: u64, options: &WalkOptions) -> JWalkIterator {
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
            })
            .into_iter();
        JWalkIterator { walk_dir_iter }
    }
}

impl Iterator for JWalkIterator {
    type Item = Result<Box<dyn Entry>, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.walk_dir_iter.next()?;
        match entry {
            Ok(entry) => Some(Ok(Box::new(JWalkEntry { entry }))),
            Err(err) => Some(Err(err.into())),
        }
    }
}
