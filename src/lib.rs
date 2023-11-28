#![cfg_attr(windows, feature(windows_by_handle))]
#![forbid(unsafe_code)]
#![feature(impl_trait_in_assoc_type)]

extern crate jwalk;

mod aggregate;
mod common;
mod crossdev;
pub mod fs_walk;
mod inodefilter;

pub mod traverse;

pub use aggregate::aggregate;
pub use common::*;
pub(crate) use inodefilter::InodeFilter;
