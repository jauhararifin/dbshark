use crate::bins::SliceExt;
use crate::btree::{BTree, Cursor};
use crate::id::{PageId, PageIdExt, TxId};
use crate::log::{TxState, WalEntry, WalKind};
use crate::pager::{LogContext, PageOps, Pager};
use crate::recovery::{recover, undo_txn};
use crate::runtime::{
    Atomic, File, JoinHandle, Runtime, RwMutex, RwMutexReadGuard, RwMutexWriteGuard, Timer,
    TimerHandle,
};
use crate::wal::Wal;
use anyhow::anyhow;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub struct Db<R: Runtime> {
    pager: Arc<Pager<R>>,
    wal: Arc<Wal<R>>,

    tx_lock: R::RwMutex<()>,
    next_txid: R::AtomicU64,
    tx_state: Arc<R::RwMutex<TxState>>,

    timer_handle: R::TimerHandle,
    background_handle: R::JoinHandle,
    shutting_down: Arc<R::AtomicUsize>,
}

pub struct Setting {
    pub checkpoint_period: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Stat {
    pub main_bytes_read: u64,
    pub main_bytes_written: u64,
    pub double_buff_bytes_written: u64,
    pub wal_bytes_written: u64,
    wal_flushed_total: u64,
    wal_flushed_because_buffer_almost_full: u64,
    wal_flushed_because_buffer_full: u64,
    wal_flushed_because_manual_trigger: u64,
    wal_flushed_because_sync_request: u64,
}

impl std::default::Default for Setting {
    fn default() -> Self {
        Self {
            checkpoint_period: Duration::from_secs(60 * 60),
        }
    }
}

impl Setting {
    fn validate(&self) -> anyhow::Result<()> {
        if self.checkpoint_period.as_secs() < 5 {
            return Err(anyhow!("checkpoint period can't be less than 5 seconds"));
        }

        Ok(())
    }
}

impl<R: Runtime> Db<R> {
    pub fn open(path: &Path, setting: Setting) -> anyhow::Result<Self> {
        setting.validate()?;

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        if !path.is_dir() {
            return Err(anyhow!("path is not a directory"));
        }

        let db_header_path = path.join("info");
        let mut db_header_file = R::File::open(db_header_path)?;
        let header = Self::load_db_header(&mut db_header_file)?;
        drop(db_header_file);

        if header.version != 0 {
            return Err(anyhow!("unsupported database version"));
        }
        let page_size = header.page_size as usize;
        let pager = Arc::new(Pager::new(path, page_size, 100000)?);

        let result = recover(path, &pager)?;
        let wal = Arc::new(result.wal);

        let next_txid = R::AtomicU64::new(result.next_txid.get());

        // at this point, the recovery is already finished, so there is no active transaction
        let tx_state = Arc::new(R::RwMutex::new(TxState::None));

        let shutting_down = Arc::new(R::AtomicUsize::new(0));

        let (mut timer, timer_handle) = R::timer(setting.checkpoint_period);
        let background_handle = {
            let wal = wal.clone();
            let pager = pager.clone();
            let tx_state = tx_state.clone();
            let shutting_down = shutting_down.clone();
            R::spawn("checkpointer", move || {
                while timer.wait() {
                    if let Err(err) = Self::checkpoint(&pager, &wal, &tx_state) {
                        // TODO: handle the error.
                        // Maybe we can send the error to the DB, so that any next operation in the DB
                        // will return an error. If we can't flush the dirty pages, we might not be
                        // able to do anything anyway.
                        log::error!("cannot perform checkpoint: {err}");
                    }

                    if shutting_down.load() == 1 {
                        break;
                    }
                }
            })
        };

        Ok(Self {
            pager,
            wal,
            tx_lock: R::RwMutex::new(()),
            next_txid,
            tx_state,

            timer_handle,
            background_handle,
            shutting_down,
        })
    }

    fn load_db_header(f: &mut R::File) -> anyhow::Result<Header> {
        let size = f.len()?;
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

    fn checkpoint(
        pager: &Pager<R>,
        wal: &Wal<R>,
        tx_state: &R::RwMutex<TxState>,
    ) -> anyhow::Result<()> {
        let tx_state = tx_state.read();
        // Warning: it is important that tx_state is locked first before
        // db_state because there might be concurrent rollback running during
        // checkpoint. The rollback process will lock the tx_state the whole
        // time. Occassionally, the rollback process might lock the db_state
        // as well. As you can see, the rollback process could lock tx_state first
        // and db_state later. If in this checkpoint process we lock the db_state
        // first then the tx_state, we might get a deadlock.
        let db_state = pager.read_state();
        let checkpoint_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: *tx_state,
                root: db_state.root,
                freelist: db_state.freelist,
                page_count: db_state.page_count,
            },
        })?;
        drop(db_state);
        drop(tx_state);
        pager.checkpoint(wal)?;
        wal.complete_checkpoint(checkpoint_lsn)?;
        Ok(())
    }

    fn init_db(f: &mut R::File) -> anyhow::Result<Header> {
        let header = Header {
            version: 0,
            page_size: DEFAULT_PAGE_SIZE as u32,
        };

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        header.encode(&mut buff[..DB_HEADER_SIZE]);
        header.encode(&mut buff[DB_HEADER_SIZE..DB_HEADER_SIZE * 2]);
        f.write_all(&buff)?;

        Ok(header)
    }

    pub fn update(&self) -> anyhow::Result<Tx<R>> {
        let tx_guard = self.tx_lock.write();

        let mut tx_state = self.tx_state.write();
        self.finish_dangling_tx(&mut tx_state)?;

        let txid = self.next_txid.fetch_add(1);
        let txid = TxId::new(txid).unwrap();
        *tx_state = TxState::Active(txid);

        Tx::new(txid, self, tx_guard)
    }

    fn finish_dangling_tx(&self, tx_state: &mut TxState) -> anyhow::Result<()> {
        match *tx_state {
            TxState::None => Ok(()),
            TxState::Active(txid) => {
                log::debug!("previous transaction {txid:?} is not closed yet");

                let lsn = self.wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::Rollback { txid },
                })?;
                *tx_state = TxState::Aborting {
                    txid,
                    last_undone: lsn,
                };
                let TxState::Aborting {
                    ref mut last_undone,
                    ..
                } = &mut *tx_state
                else {
                    unreachable!();
                };

                undo_txn(&self.pager, &self.wal, txid, last_undone)?;
                self.wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::End { txid },
                })?;
                *tx_state = TxState::None;
                Ok(())
            }
            TxState::Aborting {
                txid,
                ref mut last_undone,
            } => {
                log::debug!("continue aborting previous transaction {txid:?}");

                undo_txn(&self.pager, &self.wal, txid, last_undone)?;
                self.wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::End { txid },
                })?;
                *tx_state = TxState::None;
                Ok(())
            }
            TxState::Committing(txid) => {
                log::debug!("continue committing previous transaction {txid:?}");

                let commit_lsn = self.wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::Commit { txid },
                })?;
                self.wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::End { txid },
                })?;
                self.wal.sync(commit_lsn)?;
                *tx_state = TxState::None;
                Ok(())
            }
        }
    }

    pub fn read(&self) -> anyhow::Result<ReadTx<R>> {
        let tx_guard = self.tx_lock.read();

        let mut tx_state = self.tx_state.write();
        self.finish_dangling_tx(&mut tx_state)?;

        let txid = self.next_txid.fetch_add(1);
        let txid = TxId::new(txid).unwrap();

        let tx = ReadTx::new(txid, self, tx_guard)?;
        Ok(tx)
    }

    pub fn force_checkpoint(&self) -> anyhow::Result<()> {
        Self::checkpoint(&self.pager, &self.wal, &self.tx_state)?;
        Ok(())
    }

    pub fn stat(&self) -> Stat {
        let pager_stat = self.pager.stat();
        let wal_stat = self.wal.stat();
        Stat {
            main_bytes_read: pager_stat.main_bytes_read,
            main_bytes_written: pager_stat.main_bytes_written,
            double_buff_bytes_written: pager_stat.double_buff_bytes_written,
            wal_bytes_written: wal_stat.bytes_written,

            wal_flushed_total: wal_stat.flushed_total,
            wal_flushed_because_buffer_almost_full: wal_stat.flushed_because_buffer_almost_full,
            wal_flushed_because_buffer_full: wal_stat.flushed_because_buffer_full,
            wal_flushed_because_manual_trigger: wal_stat.flushed_because_manual_trigger,
            wal_flushed_because_sync_request: wal_stat.flushed_because_sync_request,
        }
    }

    pub fn shutdown(self) -> anyhow::Result<()> {
        let shutdowned = self.shutting_down.compare_and_exchange(0, 1);
        assert!(shutdowned);

        self.timer_handle.trigger();
        self.background_handle.join();

        // Since we own self, it means there are no active transaction since active transaction
        // borrows the db. And there are no ongoing flush and checkpoint since they also borrow
        // the db. The background thread to periodically flush and perform checkpoint is also
        // finished due to the join above.
        Self::checkpoint(&self.pager, &self.wal, &self.tx_state)?;

        // Since the background thread is finished, it means it doesn't hold the WAL anymore and
        // we can take the wal from Arc
        let wal = Arc::into_inner(self.wal).unwrap();
        wal.shutdown()?;

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

const DB_HEADER_SIZE: usize = 24;
const DEFAULT_PAGE_SIZE: usize = 0x1000;
const MAGIC_HEADER: &[u8] = b"dbshark0";

struct Header {
    version: u32,
    page_size: u32,
}

impl Header {
    fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(MAGIC_HEADER);
        buff[8..12].copy_from_slice(&self.version.to_be_bytes());
        buff[12..16].copy_from_slice(&self.page_size.to_be_bytes());
        let checksum = crc64::crc64(0x1d0f, &buff[0..16]);
        buff[16..24].copy_from_slice(&checksum.to_be_bytes());
    }

    fn decode(buff: &[u8]) -> Option<Self> {
        let calculated_checksum = crc64::crc64(0x1d0f, &buff[0..DB_HEADER_SIZE - 8]);
        let checksum = buff[DB_HEADER_SIZE - 8..].read_u64();

        if calculated_checksum != checksum {
            return None;
        }

        let version = buff[8..].read_u32();
        let page_size = buff[12..].read_u32();

        Some(Self { version, page_size })
    }
}

pub struct Tx<'db, R: Runtime> {
    id: TxId,
    wal: Arc<Wal<R>>,
    pager: Arc<Pager<R>>,

    _tx_guard: RwMutexWriteGuard<'db, R, ()>,

    tx_state: &'db R::RwMutex<TxState>,
}

impl<'db, R: Runtime> Tx<'db, R> {
    fn new(
        id: TxId,
        db: &'db Db<R>,
        tx_guard: RwMutexWriteGuard<'db, R, ()>,
    ) -> anyhow::Result<Self> {
        let tx = Self {
            id,
            wal: db.wal.clone(),
            pager: db.pager.clone(),
            _tx_guard: tx_guard,
            tx_state: &db.tx_state,
        };
        tx.wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Begin { txid: tx.id },
        })?;
        Ok(tx)
    }

    pub fn bucket(&mut self, name: &str) -> anyhow::Result<Bucket<R>> {
        let root_pgid = self.init_root()?;

        let mut btree = crate::btree::new(self.id, &self.pager, &self.wal, root_pgid);
        let result = btree.get(name.as_bytes())?;

        let bucket_pgid = if let Some(result) = result {
            let result = result.get()?;
            let pgid_buff = result.value();
            let Ok(pgid) = pgid_buff.try_into() else {
                return Err(anyhow!("invalid bucket root pgid"));
            };
            let Some(pgid) = PageId::from_be_bytes(pgid) else {
                return Err(anyhow!("invalid bucket root pgid"));
            };
            pgid
        } else {
            drop(result);
            let bucket_root = self.pager.alloc(LogContext::Runtime(&self.wal), self.id)?;
            let bucket_root_id = bucket_root.id();
            drop(bucket_root);

            let b = bucket_root_id.to_be_bytes();

            btree.put(name.as_bytes(), &b)?;
            bucket_root_id
        };

        Ok(Bucket {
            btree: crate::btree::new(self.id, &self.pager, &self.wal, bucket_pgid),
        })
    }

    fn init_root(&mut self) -> anyhow::Result<PageId> {
        let root = self.pager.read_state().root;
        if let Some(pgid) = root {
            Ok(pgid)
        } else {
            let page = self.pager.alloc(LogContext::Runtime(&self.wal), self.id)?;
            let pgid = page.id();
            self.pager
                .set_state(LogContext::Runtime(&self.wal), |state| {
                    state.root = Some(pgid);
                })?;
            Ok(pgid)
        }
    }

    pub fn commit(self) -> anyhow::Result<()> {
        let mut tx_state = self.tx_state.write();
        assert_eq!(*tx_state, TxState::Active(self.id));
        let commit_lsn = self.wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Commit { txid: self.id },
        })?;
        self.wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End { txid: self.id },
        })?;
        *tx_state = TxState::None;
        drop(tx_state);

        self.wal.sync(commit_lsn)?;
        Ok(())
    }

    pub fn rollback(self) -> anyhow::Result<()> {
        log::debug!("rollback transaction txid={:?}", self.id);

        let mut tx_state = self.tx_state.write();

        // WARNING: it is important that the wal is written after
        // the tx_state is locked because if it doesn't the checkpoint
        // can run and the checkpoint record write the stale version
        // of tx_state. As a result, the log will seems backward.
        // For example, you might see a wal record that rollback a txn
        // followed by a checkpoint record saying the tx_state is Active(TxId).
        let lsn = self.wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Rollback { txid: self.id },
        })?;

        assert_eq!(*tx_state, TxState::Active(self.id));
        *tx_state = TxState::Aborting {
            txid: self.id,
            last_undone: lsn,
        };
        let TxState::Aborting {
            ref mut last_undone,
            ..
        } = &mut *tx_state
        else {
            unreachable!();
        };

        undo_txn(&self.pager, &self.wal, self.id, last_undone)?;
        self.wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End { txid: self.id },
        })?;
        *tx_state = TxState::None;

        Ok(())
    }
}

pub struct Bucket<'a, R: Runtime> {
    btree: BTree<'a, R>,
}

impl<'a, R: Runtime> Bucket<'a, R> {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.btree.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.btree.get(key)?;
        let Some(result) = result else {
            return Ok(None);
        };
        let value = result.get()?.value().to_vec();
        Ok(Some(value))
    }

    pub fn range<Rg>(&'a self, range: Rg) -> anyhow::Result<Range<'a, R>>
    where
        Rg: RangeBounds<[u8]> + 'static,
    {
        let cursor = self.btree.range(range)?;
        Ok(Range {
            error: false,
            cursor,
        })
    }
}

pub struct Range<'a, R: Runtime> {
    error: bool,
    cursor: Cursor<'a, R>,
}

impl<'a, R: Runtime> Iterator for Range<'a, R> {
    type Item = anyhow::Result<KeyValue>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.error {
            return None;
        }

        match self.cursor.next() {
            Ok(item) => item.map(|item| {
                Ok(KeyValue {
                    key: item.key().to_vec().into_boxed_slice(),
                    value: item.value().to_vec().into_boxed_slice(),
                })
            }),
            Err(err) => {
                self.error = true;
                Some(Err(err))
            }
        }
    }
}

pub struct KeyValue {
    pub key: Box<[u8]>,
    pub value: Box<[u8]>,
}

pub struct ReadTx<'db, R: Runtime> {
    txid: TxId,
    pager: Arc<Pager<R>>,
    wal: Arc<Wal<R>>,

    _tx_guard: RwMutexReadGuard<'db, R, ()>,
}

impl<'db, R: Runtime> ReadTx<'db, R> {
    fn new(txid: TxId, db: &Db<R>, tx_guard: RwMutexReadGuard<'db, R, ()>) -> anyhow::Result<Self> {
        let tx = Self {
            txid,
            pager: db.pager.clone(),
            wal: db.wal.clone(),
            _tx_guard: tx_guard,
        };
        Ok(tx)
    }

    pub fn bucket(&self, name: &str) -> anyhow::Result<Option<ReadBucket<R>>> {
        let Some(root_pgid) = self.pager.read_state().root else {
            return Ok(None);
        };

        let btree = crate::btree::new(self.txid, &self.pager, &self.wal, root_pgid);
        let result = btree.get(name.as_bytes())?;

        let Some(result) = result else {
            return Ok(None);
        };

        let result = result.get()?;
        let pgid_buff = result.value();
        let Ok(pgid) = pgid_buff.try_into() else {
            return Err(anyhow!("invalid bucket root pgid"));
        };
        let Some(pgid) = PageId::from_be_bytes(pgid) else {
            return Err(anyhow!("invalid bucket root pgid"));
        };

        Ok(Some(ReadBucket {
            btree: crate::btree::new(self.txid, &self.pager, &self.wal, pgid),
        }))
    }
}

pub struct ReadBucket<'a, R: Runtime> {
    btree: BTree<'a, R>,
}

impl<'a, R: Runtime> ReadBucket<'a, R> {
    pub fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.btree.get(key)?;
        let Some(result) = result else {
            return Ok(None);
        };
        let value = result.get()?.value().to_vec();
        Ok(Some(value))
    }

    pub fn range<Rg>(&'a self, range: Rg) -> anyhow::Result<Range<'a, R>>
    where
        Rg: RangeBounds<[u8]> + 'static,
    {
        let cursor = self.btree.range(range)?;
        Ok(Range {
            error: false,
            cursor,
        })
    }
}
