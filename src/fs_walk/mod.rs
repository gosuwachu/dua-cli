use byte_unit::{n_gb_bytes, n_gib_bytes, n_mb_bytes, n_mib_bytes, ByteUnit};
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

pub mod jwalk;

#[cfg(test)]
pub mod mocks;

#[derive(Clone, Copy, Default)]
pub enum ByteFormat {
    /// metric format, based on 1000.
    #[default]
    Metric,
    /// binary format, based on 1024
    Binary,
    /// raw bytes, without additional formatting
    Bytes,
    /// only gigabytes without smart-unit
    GB,
    /// only gibibytes without smart-unit
    GiB,
    /// only megabytes without smart-unit
    MB,
    /// only mebibytes without smart-unit
    MiB,
}

impl ByteFormat {
    pub fn width(self) -> usize {
        use ByteFormat::*;
        match self {
            Metric => 10,
            Binary => 11,
            Bytes => 12,
            MiB | MB => 12,
            _ => 10,
        }
    }
    pub fn total_width(self) -> usize {
        use ByteFormat::*;
        const THE_SPACE_BETWEEN_UNIT_AND_NUMBER: usize = 1;

        self.width()
            + match self {
                Binary | MiB | GiB => 3,
                Metric | MB | GB => 2,
                Bytes => 1,
            }
            + THE_SPACE_BETWEEN_UNIT_AND_NUMBER
    }
    pub fn display(self, bytes: u128) -> ByteFormatDisplay {
        ByteFormatDisplay {
            format: self,
            bytes,
        }
    }
}

pub struct ByteFormatDisplay {
    format: ByteFormat,
    bytes: u128,
}

impl fmt::Display for ByteFormatDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use byte_unit::Byte;
        use ByteFormat::*;

        let format = match self.format {
            Bytes => return write!(f, "{} b", self.bytes),
            Binary => (true, None),
            Metric => (false, None),
            GB => (false, Some((n_gb_bytes!(1), ByteUnit::GB))),
            GiB => (false, Some((n_gib_bytes!(1), ByteUnit::GiB))),
            MB => (false, Some((n_mb_bytes!(1), ByteUnit::MB))),
            MiB => (false, Some((n_mib_bytes!(1), ByteUnit::MiB))),
        };

        let b = match format {
            (_, Some((divisor, unit))) => Byte::from_unit(self.bytes as f64 / divisor as f64, unit)
                .expect("byte count > 0")
                .get_adjusted_unit(unit),
            (binary, None) => Byte::from_bytes(self.bytes).get_appropriate_unit(binary),
        }
        .format(2);
        let mut splits = b.split(' ');
        match (splits.next(), splits.next()) {
            (Some(bytes), Some(unit)) => write!(
                f,
                "{} {:>unit_width$}",
                bytes,
                unit,
                unit_width = match self.format {
                    Binary => 3,
                    Metric => 2,
                    _ => 2,
                }
            ),
            _ => f.write_str(&b),
        }
    }
}

/// Identify the kind of sorting to apply during filesystem iteration
#[derive(Clone, Default)]
pub enum TraversalSorting {
    #[default]
    None,
    AlphabeticalByFileName,
}

/// Configures a filesystem walk, including output and formatting options.
#[derive(Clone, Default)]
pub struct WalkOptions {
    /// The amount of threads to use. Refer to [`WalkDir::num_threads()`](https://docs.rs/jwalk/0.4.0/jwalk/struct.WalkDir.html#method.num_threads)
    /// for more information.
    pub threads: usize,
    pub byte_format: ByteFormat,
    pub count_hard_links: bool,
    pub apparent_size: bool,
    pub sorting: TraversalSorting,
    pub cross_filesystems: bool,
    pub ignore_dirs: Vec<PathBuf>,
}

pub trait Metadata {
    fn is_dir(&self) -> bool;
    fn dev(&self) -> u64;
    fn ino(&self) -> u64;
    fn nlink(&self) -> u64;
    fn apparent_size(&self) -> u64;
    fn size_on_disk(&self) -> io::Result<u64>;
    fn modified(&self) -> io::Result<SystemTime>;
}

pub trait Entry {
    fn depth(&self) -> usize;
    fn path(&self) -> PathBuf;
    fn file_name(&self) -> PathBuf;
    fn parent_path(&self) -> PathBuf;
    fn metadata(&self) -> Option<Result<impl Metadata + '_, io::Error>>;
}

pub trait Walker {
    fn device_id(&self, path: &Path) -> io::Result<u64>;

    fn into_iter(
        &mut self,
        path: &Path,
        root_device_id: u64,
    ) -> impl Iterator<Item = Result<impl Entry + Debug, io::Error>>;
}
