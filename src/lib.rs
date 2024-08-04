// TODO: don't use anyhow for the error handling
// TODO: setup CI

mod btree;
mod content;
mod db;
mod pager;
mod recovery;
mod wal;

pub use db::{Db, Setting};
