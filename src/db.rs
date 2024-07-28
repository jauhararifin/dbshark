use crate::btree::BTree;
use crate::pager::{PageId, PageIdExt, Pager};
use crate::recovery::{recover, undo_txn};
use crate::wal::{TxId, TxIdExt, TxState, Wal, WalRecord};
use anyhow::anyhow;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{fs::File, path::Path};

pub struct Db {
    internal: RwLock<DbInternal>,
    pager: Arc<Pager>,
    wal: Arc<Wal>,

    tx_state: Arc<RwLock<DbTxState>>,

    background_chan: Sender<()>,
    background_thread: JoinHandle<()>,
}

struct DbInternal {
    // TODO: maybe these two information should be managed by Pager
    root: Option<PageId>,
    freelist: Option<PageId>,
}

struct DbTxState {
    tx_state: TxState,
    next_txid: TxId,
}

#[derive(Default)]
pub struct Setting {}

impl Db {
    pub fn open(path: &Path, setting: Setting) -> anyhow::Result<Self> {
        log::debug!("opening db on {path:?}");
        let wal_path = path.with_extension("wal");
        let double_buff_path = path.with_extension("dbuff");

        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let header = Self::load_db_header(&mut db_file)?;

        let double_buff_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(double_buff_path)?;

        if header.version != 0 {
            return Err(anyhow!("unsupported database version"));
        }
        let page_size = header.page_size as usize;
        let pager = Arc::new(Pager::new(db_file, double_buff_file, page_size, 1000)?);

        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(wal_path)?;
        let wal = recover(wal_file, &pager, page_size)?;
        let wal = Arc::new(wal);

        let next_txid = if let Some(txid) = header.last_txid {
            txid.next()
        } else {
            TxId::new(1).unwrap()
        };

        let tx_state = Arc::new(RwLock::new(DbTxState {
            // at this point, the recovery is already finished, so there is no active transaction
            tx_state: TxState::None,
            next_txid,
        }));

        let (sender, receiver) = channel();
        let background_thread = {
            let wal = wal.clone();
            let pager = pager.clone();
            let tx_state = tx_state.clone();
            std::thread::spawn(move || loop {
                let Err(err) = receiver.recv_timeout(Duration::from_secs(60 * 60)) else {
                    break;
                };
                if err != std::sync::mpsc::RecvTimeoutError::Timeout {
                    break;
                }

                if let Err(err) = Self::checkpoint(&pager, &wal, &tx_state) {
                    // TODO: handle the error.
                    // Maybe we can send the error to the DB, so that any next operation in the DB
                    // will return an error. If we can't flush the dirty pages, we might not be
                    // able to do anything anyway.
                    todo!()
                }
            })
        };

        Ok(Self {
            pager,
            wal,
            internal: RwLock::new(DbInternal {
                root: None,
                freelist: None,
            }),

            tx_state,
            background_chan: sender,
            background_thread,
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

    fn checkpoint(pager: &Pager, wal: &Wal, tx_state: &RwLock<DbTxState>) -> anyhow::Result<()> {
        let checkpoint_lsn = {
            let tx_state = tx_state.read();
            wal.append(
                TxId::new(1).unwrap(),
                None,
                WalRecord::CheckpointBegin {
                    active_tx: tx_state.tx_state,
                },
            )?
        };
        pager.checkpoint(&wal)?;
        wal.update_checkpoint(checkpoint_lsn)?;
        Ok(())
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
        let mut tx_state = self.tx_state.write();
        if let TxState::Active(txid) = tx_state.tx_state {
            log::debug!("previous transaction {txid:?} is not closed yet");

            let lsn = self.wal.append(txid, None, WalRecord::Rollback)?;
            tx_state.tx_state = TxState::Aborting {
                txid,
                rollback: lsn,
            };

            {
                let mut db = self.internal.write();
                let mut db: &mut DbInternal = &mut db;
                undo_txn(
                    &self.pager,
                    &self.wal,
                    txid,
                    lsn,
                    lsn,
                    &mut db.root,
                    &mut db.freelist,
                )?;
                self.wal.append(txid, None, WalRecord::End)?;
            }

            tx_state.tx_state = TxState::None;
        }

        let txid = tx_state.next_txid;
        tx_state.next_txid = tx_state.next_txid.next();
        tx_state.tx_state = TxState::Active(txid);

        Tx::new(txid, self)
    }

    pub fn force_checkpoint(&self) -> anyhow::Result<()> {
        Self::checkpoint(&self.pager, &self.wal, &self.tx_state)?;
        Ok(())
    }

    pub fn shutdown(self) -> anyhow::Result<()> {
        let internal = self.internal.into_inner();
        self.background_chan.send(())?;
        if self.background_thread.join().is_err() {
            return Err(anyhow!("cannot join background thread"));
        }

        // Since we own self, it means there are no active transaction since active transaction
        // borrows the db. And there are no ongoing flush and checkpoint since they also borrow
        // the db. The background thread to periodically flush and perform checkpoint is also
        // finished due to the join above. So, there is only one reference to the pager.
        let pager = Arc::into_inner(self.pager).expect(
            "there should only be one reference to pager after the background thread is returned",
        );
        pager.shutdown()?;

        Ok(())
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
    wal: Arc<Wal>,
    pager: Arc<Pager>,

    tx_state: &'db RwLock<DbTxState>,
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
    fn new(id: TxId, db: &'db Db) -> anyhow::Result<Self> {
        let tx = Self {
            id,
            wal: db.wal.clone(),
            pager: db.pager.clone(),
            tx_state: &db.tx_state,
            db: db.internal.write(),
            closed: false,
        };
        tx.wal.append(tx.id, None, WalRecord::Begin)?;
        Ok(tx)
    }

    pub fn bucket(&mut self, name: &str) -> anyhow::Result<Bucket> {
        let root_pgid = self.init_root()?;

        let mut btree = BTree::new(self.id, &self.pager, &self.wal, root_pgid, self.db.freelist);
        let mut result = btree.seek(name.as_bytes())?;
        let bucket_pgid = if !result.found {
            drop(result);
            let bucket_root = self.pager.alloc(self.id)?;
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
            btree: BTree::new(
                self.id,
                &self.pager,
                &self.wal,
                bucket_pgid,
                self.db.freelist,
            ),
        })
    }

    fn init_root(&mut self) -> anyhow::Result<PageId> {
        if let Some(pgid) = self.db.root {
            Ok(pgid)
        } else {
            let page = self.pager.alloc(self.id)?;
            let pgid = page.id();
            self.wal.append(
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
        {
            let mut tx_state = self.tx_state.write();
            assert_eq!(tx_state.tx_state, TxState::Active(self.id));
            tx_state.tx_state = TxState::Committing(self.id);
        }

        let commit_lsn = self.wal.append(self.id, None, WalRecord::Commit)?;
        self.wal.append(self.id, None, WalRecord::End)?;
        self.wal.sync(commit_lsn)?;
        self.closed = true;

        {
            let mut tx_state = self.tx_state.write();
            assert_eq!(tx_state.tx_state, TxState::Committing(self.id));
            tx_state.tx_state = TxState::None;
        }

        Ok(())
    }

    pub fn rollback(mut self) -> anyhow::Result<()> {
        log::debug!("rollback transaction txid={:?}", self.id);

        let lsn = {
            let lsn = self.wal.append(self.id, None, WalRecord::Rollback)?;
            let mut tx_state = self.tx_state.write();
            assert_eq!(tx_state.tx_state, TxState::Active(self.id));
            tx_state.tx_state = TxState::Aborting {
                txid: self.id,
                rollback: lsn,
            };
            lsn
        };

        let db: &mut DbInternal = &mut self.db;
        undo_txn(
            &self.pager,
            &self.wal,
            self.id,
            lsn,
            lsn,
            &mut db.root,
            &mut db.freelist,
        )?;
        self.wal.append(self.id, None, WalRecord::End)?;
        self.closed = true;

        {
            let mut tx_state = self.tx_state.write();
            tx_state.tx_state = TxState::None;
        }

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
