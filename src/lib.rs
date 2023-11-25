#![cfg_attr(windows, feature(windows_by_handle))]
#![forbid(unsafe_code)]

extern crate jwalk;

mod aggregate;
mod common;
mod crossdev;
mod inodefilter;
mod fs_walk;

pub mod traverse;

pub use aggregate::aggregate;
pub use common::*;
pub use fs_walk::*;
pub use fs_walk::jwalk::JWalkWalker;
pub(crate) use inodefilter::InodeFilter;
