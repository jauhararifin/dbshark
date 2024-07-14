use crate::btree::BTree;
use crate::pager::{PageId, PageIdExt, Pager};
use crate::wal::{TxId, TxIdExt, Wal, WalRecord};
use anyhow::anyhow;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::io::{Read, Write};
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
    root: Option<PageId>,
    freelist: Option<PageId>,
}

impl Db {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let wal_path = path.with_extension("wal");

        let mut db_file = File::create(path)?;
        let header = Self::load_db_header(&mut db_file)?;

        if header.version != 0 {
            return Err(anyhow!("unsupported database version"));
        }
        let page_size = header.page_size as usize;

        let wal_file = File::create(wal_path)?;
        let wal = Arc::new(Wal::new(wal_file, page_size));

        let pager = Pager::new(db_file, wal.clone(), page_size, 1000)?;

        let next_txid = if let Some(txid) = header.last_txid {
            txid.next()
        } else {
            TxId::new(1).unwrap()
        };

        Ok(Self {
            internal: RwLock::new(DbInternal {
                pager,
                wal,
                next_txid,
                root: None,
                freelist: None,
            }),
        })
    }

    fn load_db_header(f: &mut File) -> anyhow::Result<Header> {
        let meta = f.metadata()?;
        let size = meta.size();

        if size == 0 {
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
        let txid = internal.next_txid;
        internal.next_txid = internal.next_txid.next();

        Ok(Tx {
            id: txid,
            db: internal,
            closed: false,
        })
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
        let calculated_checksum = crc64::crc64(0, &buff[0..24]);
        let checksum = u64::from_be_bytes(buff[24..32].try_into().unwrap());

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
            todo!("should we panic here?");
        }
    }
}

impl<'db> Tx<'db> {
    // TODO: remove this. By right we should be able to create a bunch of buckets
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let root_pgid = self.init_root()?;

        let mut btree = BTree::new(self.id, &self.db.pager, root_pgid, self.db.freelist);
        btree.put(key, value)?;

        Ok(())
    }

    fn init_root(&mut self) -> anyhow::Result<PageId> {
        if let Some(pgid) = self.db.root {
            Ok(pgid)
        } else {
            let page = self.db.pager.alloc(self.id)?;
            let pgid = page.id();
            self.db.wal.append(
                self.id,
                WalRecord::HeaderSet {
                    root: Some(pgid),
                    freelist: self.db.freelist,
                },
            )?;
            drop(page);
            self.db.root = Some(pgid);
            Ok(pgid)
        }
    }

    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(root_pgid) = self.db.root else {
            return Ok(None);
        };

        let mut btree = BTree::new(self.id, &self.db.pager, root_pgid, self.db.freelist);
        let mut result = btree.seek(key)?;
        if !result.found {
            return Ok(None);
        }

        Ok(result.cursor.next()?.map(|item| item.value().to_vec()))
    }

    pub fn commit(mut self) {
        self.closed = true;
        // TODO: impl
    }

    pub fn rollback(mut self) {
        self.closed = true;
        // TODO: impl
    }
}
