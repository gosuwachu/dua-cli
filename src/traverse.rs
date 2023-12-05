use crate::fs_walk::{WalkOptions, Walker, Entry, Metadata};
use crate::{crossdev, get_size_or_panic, InodeFilter, Throttle};
use anyhow::Result;
use petgraph::{graph::NodeIndex, stable_graph::StableGraph, Directed, Direction};
use std::io::Write;
use std::{
    fs::File,
    fmt,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub type TreeIndex = NodeIndex;
pub type Tree = StableGraph<EntryData, (), Directed>;

#[derive(Eq, PartialEq, Clone)]
pub struct EntryData {
    pub name: PathBuf,
    /// The entry's size in bytes. If it's a directory, the size is the aggregated file size of all children
    pub size: u128,
    pub mtime: SystemTime,
    /// If set, the item meta-data could not be obtained
    pub metadata_io_error: bool,
}

impl EntryData {
    pub fn default() -> EntryData {
        EntryData {
            name: PathBuf::default(),
            size: u128::default(),
            mtime: UNIX_EPOCH,
            metadata_io_error: bool::default(),
        }
    }
}

impl fmt::Debug for EntryData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryData")
            .field("name", &self.name)
            .field("size", &self.size)
            // Skip mtime
            .field("metadata_io_error", &self.metadata_io_error)
            .finish()
    }
}

/// The result of the previous filesystem traversal
#[derive(Debug)]
pub struct Traversal {
    /// A tree representing the entire filestem traversal
    pub tree: Tree,
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

impl Traversal {
    pub fn from_walker(
        mut walker: impl Walker,
        mut walk_options: WalkOptions,
        input: Vec<PathBuf>,
        mut update: impl FnMut(&mut Traversal) -> Result<bool>,
    ) -> Result<Option<Traversal>> {
        let mut log = File::create("log.txt").unwrap();

        let mut t = {
            let mut tree = Tree::new();
            let root_index = tree.add_node(EntryData::default());
            Traversal {
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
        let mut sizes_per_depth_level = Vec::new();
        let mut current_size_at_depth: u128 = 0;
        let mut previous_depth = 0;
        let mut inodes = InodeFilter::default();

        let throttle = Throttle::new(Duration::from_millis(250), None);
        if walk_options.threads == 0 {
            // avoid using the global rayon pool, as it will keep a lot of threads alive after we are done.
            // Also means that we will spin up a bunch of threads per root path, instead of reusing them.
            walk_options.threads = num_cpus::get();
        }

        for path in input.into_iter() {
            let device_id = match walker.device_id(path.as_ref()) {
                Ok(id) => id,
                Err(_) => {
                    t.io_errors += 1;
                    continue;
                }
            };
            for entry in walker.into_iter(path.as_ref(), device_id) {
                t.entries_traversed += 1;
                let mut data = EntryData::default();

                log.write_all(format!("{:#?}\n", entry).as_bytes()).unwrap();

                match entry {
                    Ok(entry) => {
                        data.name = if entry.depth() < 1 {
                            path.clone()
                        } else {
                            entry.file_name().into()
                        };

                        let mut file_size = 0u128;
                        let mut mtime: SystemTime = UNIX_EPOCH;
                        match &entry.metadata() {
                            Some(Ok(ref m)) => {
                                if !m.is_dir()
                                    && (walk_options.count_hard_links
                                        || inodes.add_raw(m.dev(), m.ino(), m.nlink()))
                                    && (walk_options.cross_filesystems
                                        || crossdev::is_same_device_raw(device_id, m.dev()))
                                {
                                    if walk_options.apparent_size {
                                        file_size = m.apparent_size() as u128;
                                    } else {
                                        file_size = m.size_on_disk().unwrap_or_else(|_| {
                                            t.io_errors += 1;
                                            data.metadata_io_error = true;
                                            0
                                        })
                                            as u128;
                                    }
                                }

                                match m.modified() {
                                    Ok(modified) => {
                                        mtime = modified;
                                    }
                                    Err(_) => {
                                        t.io_errors += 1;
                                        data.metadata_io_error = true;
                                    }
                                }
                            }
                            Some(Err(_)) => {
                                t.io_errors += 1;
                                data.metadata_io_error = true;
                            }
                            None => {}
                        }

                        match (entry.depth(), previous_depth) {
                            (n, p) if n > p => {
                                sizes_per_depth_level.push(current_size_at_depth);
                                current_size_at_depth = file_size;
                                parent_node_idx = previous_node_idx;
                            }
                            (n, p) if n < p => {
                                for _ in n..p {
                                    set_size_or_panic(
                                        &mut t.tree,
                                        parent_node_idx,
                                        current_size_at_depth,
                                    );
                                    current_size_at_depth +=
                                        pop_or_panic(&mut sizes_per_depth_level);
                                    parent_node_idx = parent_or_panic(&mut t.tree, parent_node_idx);
                                }
                                current_size_at_depth += file_size;
                                set_size_or_panic(
                                    &mut t.tree,
                                    parent_node_idx,
                                    current_size_at_depth,
                                );
                            }
                            _ => {
                                current_size_at_depth += file_size;
                            }
                        };

                        data.mtime = mtime;
                        data.size = file_size;
                        let entry_index = t.tree.add_node(data);

                        t.tree.add_edge(parent_node_idx, entry_index, ());
                        previous_node_idx = entry_index;
                        previous_depth = entry.depth();
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

        sizes_per_depth_level.push(current_size_at_depth);
        current_size_at_depth = 0;
        for _ in 0..previous_depth {
            current_size_at_depth += pop_or_panic(&mut sizes_per_depth_level);
            set_size_or_panic(&mut t.tree, parent_node_idx, current_size_at_depth);
            parent_node_idx = parent_or_panic(&mut t.tree, parent_node_idx);
        }
        let root_size = t.recompute_root_size();
        set_size_or_panic(&mut t.tree, t.root_index, root_size);
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

fn set_size_or_panic(tree: &mut Tree, node_idx: TreeIndex, current_size_at_depth: u128) {
    tree.node_weight_mut(node_idx)
        .expect("node for parent index we just retrieved")
        .size = current_size_at_depth;
}

fn parent_or_panic(tree: &mut Tree, parent_node_idx: TreeIndex) -> TreeIndex {
    tree.neighbors_directed(parent_node_idx, Direction::Incoming)
        .next()
        .expect("every node in the iteration has a parent")
}

fn pop_or_panic(v: &mut Vec<u128>) -> u128 {
    v.pop().expect("sizes per level to be in sync with graph")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_walk::mocks::*;
    use std::collections::HashMap;
    use std::io;
    use crate::interactive::app::tests::utils::{debug, make_add_node};

    #[test]
    fn size_of_entry_data() {
        assert_eq!(
            std::mem::size_of::<EntryData>(),
            if cfg!(windows) { 64 } else { 64 },
            "the size of this should not change unexpectedly as it affects overall memory consumption"
        );
    }

    struct DirectoryDesc {
        name: String
    }

    struct FileDesc {
        name: String,
        size: u64
    }

    enum Entry {
        Directory(DirectoryDesc, Vec<Entry>),
        File(FileDesc)
    }

    struct Root {
        name: String,
        entries: Vec<Entry>
    }

    fn build_entries(
        entry_list: &mut Vec<Result<MockEntry, io::Error>>, 
        list: &Vec<Entry>, 
        depth: usize, 
        parent_path: PathBuf) {
        for entry in list {
            match entry {
                Entry::Directory(desc, items) => {
                    let mut path = parent_path.clone();
                    path.push(desc.name.clone());

                    entry_list.push(Ok(MockEntry { 
                        dept: depth, 
                        path: path.clone(),
                        file_name: desc.name.clone().into(),
                        parent_path: parent_path.clone(), 
                        metadata: Some(Ok(MockMetadata { 
                            is_dir: true,
                            size_on_disk: Ok(0), 
                            ..Default::default() }))
                    }));

                    build_entries(entry_list, items, depth + 1, path);
                },
                Entry::File(desc) => {
                    let mut path = parent_path.clone();
                    path.push(desc.name.clone());

                    entry_list.push(Ok(MockEntry { 
                        dept: depth, 
                        path: path,
                        file_name: desc.name.clone().into(),
                        parent_path: parent_path.clone(), 
                        metadata: Some(Ok(MockMetadata { 
                            is_dir: false,
                            size_on_disk: Ok(desc.size), 
                            ..Default::default() }))
                    }));
                }
            }
        }
    }

    fn build(roots: Vec<Root>) -> HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();

        for root in roots {
            let mut entry_list: Vec<Result<MockEntry, io::Error>> = Vec::new();
            build_entries(&mut entry_list, &root.entries, 0, "".into());
            entries.insert(root.name.into(), entry_list);
        }

        return entries;
    }

    #[test]
    fn test_from_walker() {
        let walker = MockWalker { device_id: Ok(0), entries: build(vec![
            Root {
                name: "test".into(),
                entries: vec![
                    Entry::Directory(DirectoryDesc { name: "test".into() }, vec![
                        Entry::Directory(DirectoryDesc { name: "a".into() }, vec![
                            Entry::File(FileDesc { name: "a.txt".into(), size: 10 })
                        ]),
                        Entry::File(FileDesc { name: "b.txt".into(), size: 11 })
                    ])
                ]
            }
        ])};
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            let rn = add_node("", 21, None);
            {
                let sn = add_node("test", 21, Some(rn));
                {
                    let sn = add_node("a", 10, Some(sn));
                    {
                        add_node("a.txt", 10, Some(sn));
                    }
                }
                add_node("b.txt", 11, Some(sn));
            }
        }

        assert_eq!(t.entries_traversed, 4);
        assert_eq!(t.io_errors, 0);
        assert_eq!(t.total_bytes.unwrap(), 21);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }

    #[test]
    fn test_from_walker_when_device_id_error_then_empty_tree() {
        let entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        let walker = MockWalker { device_id: Err(io::Error::from(io::ErrorKind::Other)), entries };
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            add_node("", 0, None);
        }

        assert_eq!(t.entries_traversed, 0);
        assert_eq!(t.io_errors, 1);
        assert_eq!(t.total_bytes.unwrap(), 0);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }

    #[test]
    fn test_from_walker_when_entry_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "test".into(),
                path: "test".into(),
                parent_path: "".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(0), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 1,
                file_name: "test".into(),
                path: "test/test".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(0), 
                    ..Default::default() 
                }))
            }),
            Err(io::Error::from(io::ErrorKind::Other)),
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            let rn = add_node("", 0, None);
            {
                let sn = add_node("test", 0, Some(rn));
                {
                    let sn = add_node("test", 0, Some(sn));
                }
            }
        }

        assert_eq!(t.entries_traversed, 3);
        assert_eq!(t.io_errors, 1);
        assert_eq!(t.total_bytes.unwrap(), 0);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }

    fn test_from_walker_when_entry_error_for_second_root() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "test".into(),
                path: "test".into(),
                parent_path: "".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(0), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 1,
                file_name: "test".into(),
                path: "test/test".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(0), 
                    ..Default::default() 
                }))
            }),
        ]);

        entries.insert("test1".into(), vec![
            Err(io::Error::from(io::ErrorKind::Other)),
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into(), "test1".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            let rn = add_node("", 0, None);
            {
                let sn = add_node("test", 0, Some(rn));
                {
                    let sn = add_node("test", 0, Some(sn));
                }
            }
        }

        assert_eq!(t.entries_traversed, 3);
        assert_eq!(t.io_errors, 1);
        assert_eq!(t.total_bytes.unwrap(), 0);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }

    #[test]
    fn test_from_walker_when_root_entry_error() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Err(io::Error::from(io::ErrorKind::Other)),
        ]);

        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            let rn = add_node("", 0, None);
            {
                let sn = add_node("test", 0, Some(rn));
            }
        }

        assert_eq!(t.entries_traversed, 1);
        assert_eq!(t.io_errors, 1);
        assert_eq!(t.total_bytes.unwrap(), 0);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }

    #[test]
    fn test_from_walker_root_twice() {
        let mut entries: HashMap<PathBuf, Vec<Result<MockEntry, io::Error>>> = HashMap::new();
        entries.insert("test".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "test".into(),
                path: "test".into(),
                parent_path: "".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 1,
                file_name: "a".into(),
                path: "test/a".into(),
                parent_path: "test".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 2,
                file_name: "a.txt".into(),
                path: "test/a/a.txt".into(),
                parent_path: "a".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            })
        ]);
        entries.insert("test1".into(), vec![
            Ok(MockEntry{
                dept: 0,
                file_name: "test1".into(),
                path: "test1".into(),
                parent_path: "".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 1,
                file_name: "a".into(),
                path: "test1/a".into(),
                parent_path: "test1".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: true, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            }),
            Ok(MockEntry{
                dept: 2,
                file_name: "a.txt".into(),
                path: "test1/a/a.txt".into(),
                parent_path: "a".into(),
                metadata: Some(Ok(MockMetadata { 
                    is_dir: false, 
                    size_on_disk: Ok(10), 
                    ..Default::default() }))
            })
        ]);
        
        let walker = MockWalker { device_id: Ok(0), entries };
        let walk_options = WalkOptions::default();

        let t = Traversal::from_walker(
            walker,
            walk_options,
            vec!["test".into(), "test1".into()],
            |_traversal| {
                Ok(false)
            }
        ).unwrap().unwrap();

        let mut tree = Tree::new();
        {
            let mut add_node = make_add_node(&mut tree);
            let rn = add_node("", 20, None);
            {
                let sn = add_node("test", 10, Some(rn));
                {
                    let sn = add_node("a", 10, Some(sn));
                    {
                        add_node("a.txt", 10, Some(sn));
                    }
                }
                let sn = add_node("test1", 10, Some(rn));
                {
                    let sn = add_node("a", 10, Some(sn));
                    {
                        add_node("a.txt", 10, Some(sn));
                    }
                }
            }
        }

        assert_eq!(t.entries_traversed, 6);
        assert_eq!(t.io_errors, 0);
        assert_eq!(t.total_bytes.unwrap(), 20);
        assert_eq!(
            debug(t.tree), 
            debug(tree)
        );
    }
}
