use crate::{crossdev, InodeFilter, Throttle, WalkOptions};
use anyhow::Result;
use filesize::PathExt;
use log::info;
use petgraph::{graph::NodeIndex, stable_graph::StableGraph, Directed, Direction};
use std::{
    fmt,
    fs::Metadata,
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub type TreeIndex = NodeIndex;
pub type RefreshTree = StableGraph<RefreshEntryData, (), Directed>;

#[derive(Eq, PartialEq, Clone)]
pub struct RefreshEntryData {
    pub name: PathBuf,
    /// The entry's size in bytes. If it's a directory, the size is the aggregated file size of all children
    pub size: u128,
    pub mtime: SystemTime,
    pub entry_count: Option<u64>,
    pub is_dir: bool,
    pub is_complete: bool,
    pub is_visited: bool,
}

impl Default for RefreshEntryData {
    fn default() -> RefreshEntryData {
        RefreshEntryData {
            name: PathBuf::default(),
            size: u128::default(),
            mtime: UNIX_EPOCH,
            entry_count: None,
            is_dir: false,
            is_complete: false,
            is_visited: false,
        }
    }
}

impl fmt::Debug for RefreshEntryData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryData")
            .field("name", &self.name)
            .field("size", &self.size)
            .field("entry_count", &self.entry_count)
            // Skip mtime
            .finish()
    }
}

/// The result of the previous filesystem traversal
#[derive(Debug)]
pub struct Refresh {
    /// A tree representing the entire filestem traversal
    pub tree: RefreshTree,
    /// The top-level node of the tree.
    pub root_index: TreeIndex,
    /// Amount of files or directories we have seen during the filesystem traversal
    pub entries_traversed: u64,
    /// The time at which the traversal started.
    pub start: std::time::Instant,
    /// The amount of time it took to finish the traversal. Set only once done.
    pub elapsed: Option<std::time::Duration>,
    /// Total amount of IO errors encountered when traversing the filesystem
    pub io_errors: u64,
    /// Total amount of bytes seen during the traversal
    pub total_bytes: Option<u128>,
}

#[derive(Default, Copy, Clone)]
struct EntryInfo {
    size: u128,
    entries_count: Option<u64>,
}

impl EntryInfo {
    fn add_count(&mut self, other: &Self) {
        self.entries_count = match (self.entries_count, other.entries_count) {
            (Some(a), Some(b)) => Some(a + b),
            (None, Some(b)) => Some(b),
            (Some(a), None) => Some(a),
            (None, None) => None,
        };
    }
}

fn set_entry_info_or_panic(
    tree: &mut RefreshTree,
    node_idx: TreeIndex,
    EntryInfo {
        size,
        entries_count,
    }: EntryInfo,
) {
    let node = tree
        .node_weight_mut(node_idx)
        .expect("node for parent index we just retrieved");
    node.size = size;
    node.entry_count = entries_count;
    assert!(node.is_dir);
    node.is_complete = true;
}

fn parent_or_panic(tree: &mut RefreshTree, parent_node_idx: TreeIndex) -> TreeIndex {
    tree.neighbors_directed(parent_node_idx, Direction::Incoming)
        .next()
        .expect("every node in the iteration has a parent")
}

fn pop_or_panic(v: &mut Vec<EntryInfo>) -> EntryInfo {
    v.pop().expect("sizes per level to be in sync with graph")
}

#[cfg(not(windows))]
fn size_on_disk(_parent: &Path, name: &Path, meta: &Metadata) -> io::Result<u64> {
    name.size_on_disk_fast(meta)
}

#[cfg(windows)]
fn size_on_disk(parent: &Path, name: &Path, meta: &Metadata) -> io::Result<u64> {
    parent.join(name).size_on_disk_fast(meta)
}

impl Refresh {
    pub fn from_walk(
        mut walk_options: WalkOptions,
        input: Vec<PathBuf>,
        mut update: impl FnMut(&mut Refresh) -> Result<bool>,
    ) -> Result<Option<Refresh>> {
        let mut t = {
            let mut tree = RefreshTree::new();
            let root_index = tree.add_node(RefreshEntryData {
                is_dir: true,
                ..RefreshEntryData::default()
            });
            Refresh {
                tree,
                root_index,
                entries_traversed: 0,
                start: std::time::Instant::now(),
                elapsed: None,
                io_errors: 0,
                total_bytes: None,
            }
        };

        let (mut previous_node_idx, mut parent_node_idx) = (t.root_index, t.root_index);
        let mut directory_info_per_depth_level = Vec::new();
        let mut current_directory_at_depth = EntryInfo::default();
        let mut previous_depth = 0;
        let mut inodes = InodeFilter::default();

        let throttle = Throttle::new(Duration::from_millis(250), None);
        if walk_options.threads == 0 {
            // avoid using the global rayon pool, as it will keep a lot of threads alive after we are done.
            // Also means that we will spin up a bunch of threads per root path, instead of reusing them.
            walk_options.threads = num_cpus::get();
        }

        for path in input.into_iter() {
            let device_id = match crossdev::init(path.as_ref()) {
                Ok(id) => id,
                Err(_) => {
                    t.io_errors += 1;
                    continue;
                }
            };
            for entry in walk_options
                .iter_from_path(path.as_ref(), device_id)
                .into_iter()
            {
                t.entries_traversed += 1;
                let mut data = RefreshEntryData::default();
                match entry {
                    Ok(entry) => {
                        data.name = if entry.depth < 1 {
                            path.clone()
                        } else {
                            entry.file_name.into()
                        };

                        let mut file_size = 0u128;
                        let mut mtime: SystemTime = UNIX_EPOCH;
                        match &entry.client_state {
                            Some(Ok(ref m)) => {
                                if !m.is_dir()
                                    && (walk_options.count_hard_links || inodes.add(m))
                                    && (walk_options.cross_filesystems
                                        || crossdev::is_same_device(device_id, m))
                                {
                                    if walk_options.apparent_size {
                                        file_size = m.len() as u128;
                                    } else {
                                        file_size = size_on_disk(&entry.parent_path, &data.name, m)
                                            .unwrap_or_else(|_| {
                                                t.io_errors += 1;
                                                0
                                            })
                                            as u128;
                                    }
                                    // files are complete immediately
                                    data.is_complete = true;
                                } else {
                                    data.entry_count = Some(0);
                                    data.is_dir = true;
                                }

                                match m.modified() {
                                    Ok(modified) => {
                                        mtime = modified;
                                    }
                                    Err(_) => {
                                        t.io_errors += 1;
                                    }
                                }
                            }
                            Some(Err(_)) => {
                                t.io_errors += 1;
                                // if there is an error getting the metadata the item is complete
                                data.is_complete = true;
                            }
                            None => {}
                        }

                        match (entry.depth, previous_depth) {
                            (n, p) if n > p => {
                                directory_info_per_depth_level.push(current_directory_at_depth);
                                current_directory_at_depth = EntryInfo {
                                    size: file_size,
                                    entries_count: Some(1),
                                };
                                parent_node_idx = previous_node_idx;
                            }
                            (n, p) if n < p => {
                                for _ in n..p {
                                    // that directory is "complete" at this point
                                    set_entry_info_or_panic(
                                        &mut t.tree,
                                        parent_node_idx,
                                        current_directory_at_depth,
                                    );
                                    let dir_info =
                                        pop_or_panic(&mut directory_info_per_depth_level);

                                    current_directory_at_depth.size += dir_info.size;
                                    current_directory_at_depth.add_count(&dir_info);

                                    parent_node_idx = parent_or_panic(&mut t.tree, parent_node_idx);
                                }
                                current_directory_at_depth.size += file_size;
                                *current_directory_at_depth.entries_count.get_or_insert(0) += 1;

                                // TODO: I don't think this is necessary?
                                // set_entry_info_or_panic(
                                //     &mut t.tree,
                                //     parent_node_idx,
                                //     current_directory_at_depth,
                                // );
                            }
                            _ => {
                                current_directory_at_depth.size += file_size;
                                *current_directory_at_depth.entries_count.get_or_insert(0) += 1;
                            }
                        };

                        data.mtime = mtime;
                        data.size = file_size;
                        let entry_index = t.tree.add_node(data);

                        t.tree.add_edge(parent_node_idx, entry_index, ());

                        info!(
                            "previous_depth={} depth={} {:?}",
                            previous_depth,
                            entry.depth,
                            path_of(&t.tree, entry_index, None)
                        );

                        previous_node_idx = entry_index;
                        previous_depth = entry.depth;
                    }
                    Err(_) => {
                        if previous_depth == 0 {
                            data.name = path.clone();
                            let entry_index = t.tree.add_node(data);
                            t.tree.add_edge(parent_node_idx, entry_index, ());
                        }

                        t.io_errors += 1
                    }
                }

                if throttle.can_update() && update(&mut t)? {
                    return Ok(None);
                }
            }
        }

        directory_info_per_depth_level.push(current_directory_at_depth);
        current_directory_at_depth = EntryInfo::default();
        for _ in 0..previous_depth {
            let dir_info = pop_or_panic(&mut directory_info_per_depth_level);
            current_directory_at_depth.size += dir_info.size;
            current_directory_at_depth.add_count(&dir_info);

            set_entry_info_or_panic(&mut t.tree, parent_node_idx, current_directory_at_depth);
            parent_node_idx = parent_or_panic(&mut t.tree, parent_node_idx);
        }
        let root_size = t.recompute_root_size();
        set_entry_info_or_panic(
            &mut t.tree,
            t.root_index,
            EntryInfo {
                size: root_size,
                entries_count: (t.entries_traversed > 0).then_some(t.entries_traversed),
            },
        );
        t.total_bytes = Some(root_size);

        t.elapsed = Some(t.start.elapsed());
        Ok(Some(t))
    }

    fn recompute_root_size(&self) -> u128 {
        self.tree
            .neighbors_directed(self.root_index, Direction::Outgoing)
            .map(|idx| get_size_or_panic(&self.tree, idx))
            .sum()
    }
}

fn get_entry_or_panic(tree: &RefreshTree, node_idx: TreeIndex) -> &RefreshEntryData {
    tree.node_weight(node_idx)
        .expect("node should always be retrievable with valid index")
}

fn get_size_or_panic(tree: &RefreshTree, node_idx: TreeIndex) -> u128 {
    get_entry_or_panic(tree, node_idx).size
}

// TODO: this is copied here for debugging
fn path_of(tree: &RefreshTree, mut node_idx: TreeIndex, glob_root: Option<TreeIndex>) -> PathBuf {
    const THE_ROOT: usize = 1;
    let mut entries = Vec::new();

    let mut iter = tree.neighbors_directed(node_idx, petgraph::Incoming);
    while let Some(parent_idx) = iter.next() {
        if let Some(glob_root) = glob_root {
            if glob_root == parent_idx {
                continue;
            }
        }
        entries.push(get_entry_or_panic(tree, node_idx));
        node_idx = parent_idx;
        iter = tree.neighbors_directed(node_idx, petgraph::Incoming);
    }
    entries.push(get_entry_or_panic(tree, node_idx));
    entries
        .iter()
        .rev()
        .skip(THE_ROOT)
        .fold(PathBuf::new(), |mut acc, entry| {
            acc.push(&entry.name);
            acc
        })
}

#[cfg(test)]
mod tests {
    use log::warn;
    use petgraph::Direction::Outgoing;

    use super::*;
    use crate::traverse::{EntryData, Tree};

    #[test]
    fn test_refresh() {
        let mut tree = Tree::new();
        let mut tree_builder = TreeBuilder::new(&mut tree);
        let tree_root = tree_builder.add_dir("", |b| {
            b.add_file("a");
            b.add_dir("dir", |b| {
                b.add_file("b");
            });
        });
        println!("{tree:#?}");

        let mut refresh_tree = RefreshTree::new();
        let mut refresh_tree_builder = RefreshTreeBuilder::new(&mut refresh_tree);
        let refresh_tree_root = refresh_tree_builder.add_dir("", true, |b| {
            b.add_file("a");
            b.add_dir("dir", true, |b| {
                b.add_file("b");
            });
            b.add_file("c");
        });
        println!("{refresh_tree:#?}");

        update_tree(&mut tree, tree_root, &mut refresh_tree, refresh_tree_root);
    }

    fn update_tree(tree: &mut Tree, root: TreeIndex, refresh_tree: &mut RefreshTree, refresh_root: TreeIndex) {
        let Some(refresh_entry) = refresh_tree.node_weight(refresh_root) else {
            warn!("refresh tree index not found: {refresh_root:#?}");
            return;
        };

        // TODO: if refresh entry already visited then skip

        let tree_entry = get_tree_entry(tree, root, &refresh_entry);
        match tree_entry {
            Some(tree_entry) => {
                if tree_entry.is_dir {
                    // TODO: if refresh entry is not complete don't update the directory entry
                    // TODO: don't mark as visited if not completed.
                    // TODO: go inside the directory
                } else {
                    // TODO: update file entry
                    // TODO: mark as visited
                }
            },
            None => {
                // TODO: add subtree
            }
        }

        if refresh_entry.is_complete && !refresh_entry.is_visited {
            // TODO: delete elements that don't exist in the refresh tree
        }
    }

    fn get_tree_entry<'a>(tree: &'a Tree, parent_idx: TreeIndex, refresh_entry: &'a RefreshEntryData) -> Option<&'a EntryData> {
        for idx in tree.neighbors_directed(parent_idx, Outgoing) {
            let n = tree.node_weight(idx).unwrap();
            if n.name == refresh_entry.name && n.is_dir == refresh_entry.is_dir {
                return Some(n);
            }
        }
        None
    }

    struct TreeBuilder<'a> {
        tree: &'a mut Tree,
        parent_idx: Option<TreeIndex>,
    }

    impl<'a> TreeBuilder<'a> {
        fn new(tree: &'a mut Tree) -> TreeBuilder<'a> {
            TreeBuilder {
                tree,
                parent_idx: None,
            }
        }

        fn add_file(&mut self, name: &str) {
            let n = self.tree.add_node(EntryData {
                name: PathBuf::from(name),
                is_dir: false,
                ..Default::default()
            });
            self.tree.add_edge(self.parent_idx.unwrap(), n, ());
        }

        fn add_dir<F: FnMut(&mut TreeBuilder<'_>)>(&mut self, name: &str, mut add_fn: F) -> TreeIndex {
            let n = self.tree.add_node(EntryData {
                name: PathBuf::from(name),
                is_dir: true,
                ..Default::default()
            });
            if let Some(parent_idx) = self.parent_idx {
                self.tree.add_edge(parent_idx, n, ());
            }

            let mut adder = TreeBuilder {
                tree: self.tree,
                parent_idx: Some(n),
            };
            add_fn(&mut adder);
            n
        }
    }

    struct RefreshTreeBuilder<'a> {
        tree: &'a mut RefreshTree,
        parent_idx: Option<TreeIndex>,
    }

    impl<'a> RefreshTreeBuilder<'a> {
        fn new(tree: &'a mut RefreshTree) -> RefreshTreeBuilder<'a> {
            RefreshTreeBuilder {
                tree,
                parent_idx: None,
            }
        }

        fn add_file(&mut self, name: &str) {
            let n = self.tree.add_node(RefreshEntryData {
                name: PathBuf::from(name),
                is_dir: false,
                is_complete: true,
                ..Default::default()
            });
            self.tree.add_edge(self.parent_idx.unwrap(), n, ());
        }

        fn add_dir<F: FnMut(&mut RefreshTreeBuilder<'_>)>(
            &mut self,
            name: &str,
            is_complete: bool,
            mut add_fn: F,
        ) -> TreeIndex {
            let n = self.tree.add_node(RefreshEntryData {
                name: PathBuf::from(name),
                is_dir: true,
                is_complete: is_complete,
                ..Default::default()
            });
            if let Some(parent_idx) = self.parent_idx {
                self.tree.add_edge(parent_idx, n, ());
            }

            let mut adder = RefreshTreeBuilder {
                tree: self.tree,
                parent_idx: Some(n),
            };
            add_fn(&mut adder);
            n
        }
    }
}
