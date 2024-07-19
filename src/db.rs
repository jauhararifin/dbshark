use crate::btree::BTree;
use crate::pager::{PageId, PageIdExt, Pager};
use crate::recovery::{recover, undo_txn};
use crate::wal::{self, TxId, TxIdExt, Wal, WalRecord};
use anyhow::anyhow;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::ops::DerefMut;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::{fs::File, path::Path};

pub struct Db {
    internal: RwLock<DbInternal>,
}

struct DbInternal {
    pager: Pager,
    wal: Arc<Wal>,
    next_txid: TxId,

    // TODO: maybe these two information should be managed by Pager
    root: Option<PageId>,
    freelist: Option<PageId>,

    // if the last txn is not committed or rollback, the next time
    // a new transaction begins, we should rollback the last txn
    // first.
    last_unclosed_txn: Option<TxId>,
}

#[derive(Default)]
pub struct Setting {}

impl Db {
    pub fn open(path: &Path, setting: Setting) -> anyhow::Result<Self> {
        let wal_path = path.with_extension("wal");

        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let header = Self::load_db_header(&mut db_file)?;

        if header.version != 0 {
            return Err(anyhow!("unsupported database version"));
        }
        let page_size = header.page_size as usize;
        let pager = Pager::new(db_file, page_size, 1000)?;

        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(wal_path)?;
        let wal = recover(wal_file, &pager, page_size)?;
        let wal = Arc::new(wal);

        pager.set_wal(wal.clone());

        let next_txid = if let Some(txid) = header.last_txid {
            txid.next()
        } else {
            TxId::new(1).unwrap()
        };

        // TODO: start a background thread for periodically flush dirty pages
        // TODO: start a background thread for periodically checkpoint.
        // Maybe the checkpoint can happen right after the dirty pages are flushed.
        // TODO: figure out how to roll the WAL file after it's getting bigger.

        Ok(Self {
            internal: RwLock::new(DbInternal {
                pager,
                wal,
                next_txid,
                root: None,
                freelist: None,
                last_unclosed_txn: None,
            }),
        })
    }

    fn load_db_header(f: &mut File) -> anyhow::Result<Header> {
        let meta = f.metadata()?;
        let size = meta.size();

        if size < 2 * DB_HEADER_SIZE as u64 {
            return Self::init_db(f);
        }

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        f.read_exact(&mut buff)?;

        if buff[..8].cmp(MAGIC_HEADER).is_ne() {
            return Err(anyhow!("the db file is not a database"));
        }

        if let Some(header) = Header::decode(&buff[0..DB_HEADER_SIZE]) {
            return Ok(header);
        }

        if let Some(header) = Header::decode(&buff[DB_HEADER_SIZE..DB_HEADER_SIZE * 2]) {
            return Ok(header);
        }

        Err(anyhow!("database is corrupted, both db header are broken"))
    }

    fn init_db(f: &mut File) -> anyhow::Result<Header> {
        let header = Header {
            version: 0,
            page_size: DEFAULT_PAGE_SIZE as u32,
            last_txid: None,
            root_pgid: None,
        };

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        header.encode(&mut buff[..DB_HEADER_SIZE]);
        header.encode(&mut buff[DB_HEADER_SIZE..DB_HEADER_SIZE * 2]);
        f.write_all(&buff)?;

        Ok(header)
    }

    pub fn update(&self) -> anyhow::Result<Tx> {
        let mut internal = self.internal.write();

        if let Some(txid) = internal.last_unclosed_txn {
            todo!("rollback the last unclosed txn first");
        }

        let txid = internal.next_txid;
        internal.next_txid = internal.next_txid.next();
        internal.last_unclosed_txn = Some(txid);

        Tx::new(txid, internal)
    }
}

const DB_HEADER_SIZE: usize = 40;
const DEFAULT_PAGE_SIZE: usize = 0x1000;
const MAGIC_HEADER: &[u8] = b"dbest000";

struct Header {
    version: u32,
    page_size: u32,
    last_txid: Option<TxId>,
    root_pgid: Option<PageId>,
}

impl Header {
    fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(MAGIC_HEADER);
        buff[8..12].copy_from_slice(&self.version.to_be_bytes());
        buff[12..16].copy_from_slice(&self.page_size.to_be_bytes());
        buff[16..24].copy_from_slice(&self.last_txid.to_be_bytes());
        buff[24..32].copy_from_slice(&self.root_pgid.to_be_bytes());
        let checksum = crc64::crc64(0, &buff[0..32]);
        buff[32..40].copy_from_slice(&checksum.to_be_bytes());
    }

    fn decode(buff: &[u8]) -> Option<Self> {
        let calculated_checksum = crc64::crc64(0, &buff[0..32]);
        let checksum = u64::from_be_bytes(buff[32..40].try_into().unwrap());

        if calculated_checksum != checksum {
            return None;
        }

        let version = u32::from_be_bytes(buff[8..12].try_into().unwrap());
        let page_size = u32::from_be_bytes(buff[12..16].try_into().unwrap());
        let last_txid = TxId::from_be_bytes(buff[16..24].try_into().unwrap());
        let root_pgid = PageId::from_be_bytes(buff[24..32].try_into().unwrap());

        Some(Self {
            version,
            page_size,
            last_txid,
            root_pgid,
        })
    }
}

pub struct Tx<'db> {
    id: TxId,
    db: RwLockWriteGuard<'db, DbInternal>,

    closed: bool,
}

impl<'db> Drop for Tx<'db> {
    fn drop(&mut self) {
        if !self.closed {
            // todo!("should we panic here?");
        }
    }
}

impl<'db> Tx<'db> {
    fn new(id: TxId, db: RwLockWriteGuard<'db, DbInternal>) -> anyhow::Result<Self> {
        let tx = Self {
            id,
            db,
            closed: false,
        };
        tx.db.wal.append(tx.id, None, WalRecord::Begin)?;
        Ok(tx)
    }

    pub fn bucket(&mut self, name: &str) -> anyhow::Result<Bucket> {
        let root_pgid = self.init_root()?;

        let mut btree = BTree::new(self.id, &self.db.pager, root_pgid, self.db.freelist);
        let mut result = btree.seek(name.as_bytes())?;
        let bucket_pgid = if !result.found {
            drop(result);
            let bucket_root = self.db.pager.alloc(self.id)?;
            let bucket_root_id = bucket_root.id();
            drop(bucket_root);

            let b = bucket_root_id.to_be_bytes();

            btree.put(name.as_bytes(), &b)?;
            bucket_root_id
        } else {
            let item = result.cursor.next()?.unwrap();
            let pgid_buff = item.value();
            let Ok(pgid) = pgid_buff.try_into() else {
                return Err(anyhow!("invalid bucket root pgid"));
            };
            let Some(pgid) = PageId::from_be_bytes(pgid) else {
                return Err(anyhow!("invalid bucket root pgid"));
            };
            pgid
        };

        Ok(Bucket {
            btree: BTree::new(self.id, &self.db.pager, bucket_pgid, self.db.freelist),
        })
    }

    fn init_root(&mut self) -> anyhow::Result<PageId> {
        if let Some(pgid) = self.db.root {
            Ok(pgid)
        } else {
            let page = self.db.pager.alloc(self.id)?;
            let pgid = page.id();
            self.db.wal.append(
                self.id,
                None,
                WalRecord::HeaderSet {
                    root: Some(pgid),
                    old_root: self.db.root,
                    freelist: self.db.freelist,
                    old_freelist: self.db.freelist,
                },
            )?;
            drop(page);
            self.db.root = Some(pgid);
            Ok(pgid)
        }
    }

    pub fn commit(mut self) -> anyhow::Result<()> {
        let commit_lsn = self.db.wal.append(self.id, None, WalRecord::Commit)?;
        self.db.wal.append(self.id, None, WalRecord::End)?;
        self.db.wal.sync(commit_lsn)?;

        self.closed = true;
        self.db.last_unclosed_txn = None;
        Ok(())
    }

    pub fn rollback(mut self) -> anyhow::Result<()> {
        log::debug!("rollback transaction txid={:?}", self.id);

        let lsn = self.db.wal.append(self.id, None, WalRecord::Rollback)?;
        log::debug!("rollback log record txid={:?} lsn={lsn:?}", self.id);
        let (pager, wal, db_root, db_freelist) = {
            let db = self.db.deref_mut();
            (&db.pager, &db.wal, &mut db.root, &mut db.freelist)
        };
        undo_txn(pager, wal, self.id, lsn, db_root, db_freelist)?;
        self.db.wal.append(self.id, None, WalRecord::End)?;

        self.closed = true;
        self.db.last_unclosed_txn = None;

        Ok(())
    }
}

pub struct Bucket<'a> {
    btree: BTree<'a>,
}

impl<'a> Bucket<'a> {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.btree.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let mut result = self.btree.seek(key)?;
        if !result.found {
            return Ok(None);
        }
        let value = result.cursor.next()?.map(|item| item.value().to_vec());
        Ok(value)
    }
}
