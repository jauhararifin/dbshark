// TODO: implement deletion
// TODO: use nonce for checksum
// TODO: implement WAL rolling
// TODO: use pointer swizzling for better buffer pool
// TODO: add fast path for adding entry to a btree
// TODO: batch commit logs together
// TODO: remove txid in some wal entries
// TODO: use binary search to search in nodes
// TODO: just remove the btree lock altogether. There is no point using it if we only have one
// write txn at a time.
// TODO: use better eviction policy
// TODO: check WAL entry's integrity when iterating
// TODO: add fast path for the pager
// TODO: reduce syscall for writing files. We can use vectorized syscall
// TODO: improve checkpoint: don't exclusively lock a page if it's not dirty and need to be
// flushed.
// TODO: merge operations on the files to reduce the WAL size
// TODO: remove byte copy during rearrangement
// TODO: remove allocation during btree split
// TODO: handle checkpoint error in the background thread
// TODO: don't use anyhow for the error handling
// TODO: setup CI

mod bins;
mod btree;
mod btree_v2;
mod content;
mod db;
mod file_lock;
mod id;
mod log;
mod pager;
mod pager_v2;
mod recovery;
mod recovery_v2;
mod wal;

pub use db::{Db, Setting};
