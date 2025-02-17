use crate::bins::SliceExt;
use crate::btree::{BTree, Cursor};
use crate::id::{PageId, PageIdExt, TxId};
use crate::log::{TxState, WalEntry, WalKind};
use crate::metric::HistogramPercentile;
use crate::pager::{LogContext, PageOps, Pager};
use crate::recovery::{recover, undo_txn};
use crate::runtime::{Atomic, File, JoinHandle, Runtime, RwMutex, Timer, TimerHandle};
use crate::wal::Wal;
use anyhow::anyhow;
use std::future::Future;
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
    pub buffer_size: usize,
}

#[derive(Debug)]
pub struct Stat {
    pub main_bytes_read: u64,
    pub main_bytes_written: u64,
    pub double_buff_bytes_written: u64,
    pub wal_bytes_written: u64,

    pub wal_flushed_total: u64,
    pub wal_flushed_because_timeout: u64,
    pub wal_flushed_because_buffer_almost_full: u64,
    pub wal_flushed_because_buffer_full: u64,
    pub wal_flushed_because_manual_trigger: u64,
    pub wal_flushed_because_sync_request: u64,
    pub wal_flush_latency: HistogramPercentile,
}

impl std::default::Default for Setting {
    fn default() -> Self {
        Self {
            checkpoint_period: Duration::from_secs(60 * 60),
            buffer_size: 100000,
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
    pub async fn open(path: &Path, setting: Setting) -> anyhow::Result<Self> {
        setting.validate()?;

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        if !path.is_dir() {
            return Err(anyhow!("path is not a directory"));
        }

        let db_header_path = path.join("info");
        let mut db_header_file = R::File::open(&db_header_path).await?;
        let header = Self::load_db_header(&mut db_header_file).await?;
        drop(db_header_file);

        if header.version != 0 {
            return Err(anyhow!("unsupported database version"));
        }
        let page_size = header.page_size as usize;
        let pager = Arc::new(Pager::new(path, page_size, setting.buffer_size).await?);

        let result = recover(path, &pager).await?;
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
            R::spawn("checkpointer", async move {
                while timer.wait().await {
                    if let Err(err) = Self::checkpoint(&pager, &wal, &tx_state).await {
                        // TODO: handle the error.
                        // Maybe we can send the error to the DB, so that any next operation in the DB
                        // will return an error. If we can't flush the dirty pages, we might not be
                        // able to do anything anyway.
                        log::error!("cannot perform checkpoint: {err}");
                    }

                    if shutting_down.load().await == 1 {
                        break;
                    }
                }
            })
            .await
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

    async fn load_db_header(f: &mut R::File) -> anyhow::Result<Header> {
        let size = f.len().await?;
        if size < 2 * DB_HEADER_SIZE as u64 {
            return Self::init_db(f).await;
        }

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        f.read_exact(&mut buff).await?;

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

    async fn checkpoint(
        pager: &Pager<R>,
        wal: &Wal<R>,
        tx_state: &R::RwMutex<TxState>,
    ) -> anyhow::Result<()> {
        let tx_state = tx_state.read().await;
        // Warning: it is important that tx_state is locked first before
        // db_state because there might be concurrent rollback running during
        // checkpoint. The rollback process will lock the tx_state the whole
        // time. Occassionally, the rollback process might lock the db_state
        // as well. As you can see, the rollback process could lock tx_state first
        // and db_state later. If in this checkpoint process we lock the db_state
        // first then the tx_state, we might get a deadlock.
        let db_state = pager.read_state().await;
        let checkpoint_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: *tx_state,
                    root: db_state.root,
                    freelist: db_state.freelist,
                    page_count: db_state.page_count,
                },
            })
            .await?;
        drop(db_state);
        drop(tx_state);
        pager.checkpoint(wal).await?;
        wal.complete_checkpoint(checkpoint_lsn).await?;
        Ok(())
    }

    async fn init_db(f: &mut R::File) -> anyhow::Result<Header> {
        let header = Header {
            version: 0,
            page_size: DEFAULT_PAGE_SIZE as u32,
        };

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        header.encode(&mut buff[..DB_HEADER_SIZE]);
        header.encode(&mut buff[DB_HEADER_SIZE..DB_HEADER_SIZE * 2]);
        f.write_all(&buff).await?;

        Ok(header)
    }

    pub async fn update(&self) -> anyhow::Result<WriteTx<R>> {
        let tx_guard = self.tx_lock.write().await;

        let mut tx_state = self.tx_state.write().await;
        self.finish_dangling_tx(&mut tx_state).await?;

        let txid = self.next_txid.fetch_add(1).await;
        let txid = TxId::new(txid).unwrap();
        *tx_state = TxState::Active(txid);

        WriteTx::new(txid, self, tx_guard).await
    }

    async fn finish_dangling_tx(&self, tx_state: &mut TxState) -> anyhow::Result<()> {
        match *tx_state {
            TxState::None => Ok(()),
            TxState::Active(txid) => {
                log::debug!("previous transaction {txid:?} is not closed yet");

                let lsn = self
                    .wal
                    .append_log(WalEntry {
                        clr: None,
                        kind: WalKind::Rollback { txid },
                    })
                    .await?;
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

                undo_txn(&self.pager, &self.wal, txid, last_undone).await?;
                self.wal
                    .append_log(WalEntry {
                        clr: None,
                        kind: WalKind::End { txid },
                    })
                    .await?;
                *tx_state = TxState::None;
                Ok(())
            }
            TxState::Aborting {
                txid,
                ref mut last_undone,
            } => {
                log::debug!("continue aborting previous transaction {txid:?}");

                undo_txn(&self.pager, &self.wal, txid, last_undone).await?;
                self.wal
                    .append_log(WalEntry {
                        clr: None,
                        kind: WalKind::End { txid },
                    })
                    .await?;
                *tx_state = TxState::None;
                Ok(())
            }
            TxState::Committing(txid) => {
                log::debug!("continue committing previous transaction {txid:?}");

                let commit_lsn = self
                    .wal
                    .append_log(WalEntry {
                        clr: None,
                        kind: WalKind::Commit { txid },
                    })
                    .await?;
                self.wal
                    .append_log(WalEntry {
                        clr: None,
                        kind: WalKind::End { txid },
                    })
                    .await?;
                self.wal.sync(commit_lsn).await?;
                *tx_state = TxState::None;
                Ok(())
            }
        }
    }

    pub async fn read(&self) -> anyhow::Result<ReadTx<R>> {
        let tx_guard = self.tx_lock.read().await;

        let mut tx_state = self.tx_state.write().await;
        self.finish_dangling_tx(&mut tx_state).await?;

        let txid = self.next_txid.fetch_add(1).await;
        let txid = TxId::new(txid).unwrap();

        let tx = ReadTx::new(txid, self, tx_guard)?;
        Ok(tx)
    }

    pub async fn force_checkpoint(&self) -> anyhow::Result<()> {
        Self::checkpoint(&self.pager, &self.wal, &self.tx_state).await?;
        Ok(())
    }

    pub async fn stat(&self) -> Stat {
        let pager_stat = self.pager.stat().await;
        let wal_stat = self.wal.stat();
        Stat {
            main_bytes_read: pager_stat.main_bytes_read,
            main_bytes_written: pager_stat.main_bytes_written,
            double_buff_bytes_written: pager_stat.double_buff_bytes_written,
            wal_bytes_written: wal_stat.bytes_written,

            wal_flushed_total: wal_stat.flushed_total,
            wal_flushed_because_timeout: wal_stat.flushed_because_timeout,
            wal_flushed_because_buffer_almost_full: wal_stat.flushed_because_buffer_almost_full,
            wal_flushed_because_buffer_full: wal_stat.flushed_because_buffer_full,
            wal_flushed_because_manual_trigger: wal_stat.flushed_because_manual_trigger,
            wal_flushed_because_sync_request: wal_stat.flushed_because_sync_request,
            wal_flush_latency: wal_stat.flush_latency,
        }
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        let shutdowned = self.shutting_down.compare_and_exchange(0, 1).await;
        assert!(shutdowned);

        self.timer_handle.trigger().await;
        self.background_handle.join().await;

        // Since we own self, it means there are no active transaction since active transaction
        // borrows the db. And there are no ongoing flush and checkpoint since they also borrow
        // the db. The background thread to periodically flush and perform checkpoint is also
        // finished due to the join above.
        Self::checkpoint(&self.pager, &self.wal, &self.tx_state).await?;

        // Since the background thread is finished, it means it doesn't hold the WAL anymore and
        // we can take the wal from Arc
        let wal = Arc::into_inner(self.wal).unwrap();
        wal.shutdown().await?;

        // Since we own self, it means there are no active transaction since active transaction
        // borrows the db. And there are no ongoing flush and checkpoint since they also borrow
        // the db. The background thread to periodically flush and perform checkpoint is also
        // finished due to the join above. So, there is only one reference to the pager.
        let pager = Arc::into_inner(self.pager).expect(
            "there should only be one reference to pager after the background thread is returned",
        );
        pager.shutdown().await?;

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

pub struct WriteTx<'db, R: Runtime> {
    id: TxId,
    wal: Arc<Wal<R>>,
    pager: Arc<Pager<R>>,

    _tx_guard: <R::RwMutex<()> as RwMutex<()>>::WriteGuard<'db>,

    tx_state: &'db R::RwMutex<TxState>,
}

impl<'db, R: Runtime> WriteTx<'db, R> {
    async fn new(
        id: TxId,
        db: &'db Db<R>,
        tx_guard: <R::RwMutex<()> as RwMutex<()>>::WriteGuard<'db>,
    ) -> anyhow::Result<Self> {
        let tx = Self {
            id,
            wal: db.wal.clone(),
            pager: db.pager.clone(),
            _tx_guard: tx_guard,
            tx_state: &db.tx_state,
        };
        tx.wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Begin { txid: tx.id },
            })
            .await?;
        Ok(tx)
    }

    // TODO: figure out a way to acquire multiple bucket without requerying like:
    // a = bucket("xxx")
    // b = bucket("yyy")
    // c = buckey("zzz")
    // a.put(...)
    // b.put(...)
    // a.put(...) // this fails because a is already dropped after b is used
    // c.put(...)
    pub async fn bucket(&mut self, name: &str) -> anyhow::Result<WriteBucket<R>> {
        let root_pgid = self.init_root().await?;

        let mut btree = crate::btree::new(self.id, &self.pager, &self.wal, root_pgid);
        let result = btree.get(name.as_bytes()).await?;

        let bucket_pgid = if let Some(result) = result {
            let result = result.get().await?;
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
            let bucket_root = self
                .pager
                .alloc(LogContext::Runtime(&self.wal), self.id)
                .await?;
            let bucket_root_id = bucket_root.id();
            drop(bucket_root);

            let b = bucket_root_id.to_be_bytes();

            btree.put(name.as_bytes(), &b).await?;
            bucket_root_id
        };

        Ok(WriteBucket {
            btree: crate::btree::new(self.id, &self.pager, &self.wal, bucket_pgid),
        })
    }

    async fn init_root(&mut self) -> anyhow::Result<PageId> {
        let root = self.pager.read_state().await.root;
        if let Some(pgid) = root {
            Ok(pgid)
        } else {
            let page = self
                .pager
                .alloc(LogContext::Runtime(&self.wal), self.id)
                .await?;
            let pgid = page.id();
            self.pager
                .set_state(LogContext::Runtime(&self.wal), |state| {
                    state.root = Some(pgid);
                })
                .await?;
            Ok(pgid)
        }
    }

    pub async fn commit(self) -> anyhow::Result<()> {
        let mut tx_state = self.tx_state.write().await;
        assert_eq!(*tx_state, TxState::Active(self.id));
        let commit_lsn = self
            .wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Commit { txid: self.id },
            })
            .await?;
        self.wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::End { txid: self.id },
            })
            .await?;
        *tx_state = TxState::None;
        drop(tx_state);

        self.wal.sync(commit_lsn).await?;
        Ok(())
    }

    pub async fn rollback(self) -> anyhow::Result<()> {
        log::debug!("rollback transaction txid={:?}", self.id);

        let mut tx_state = self.tx_state.write().await;

        // WARNING: it is important that the wal is written after
        // the tx_state is locked because if it doesn't the checkpoint
        // can run and the checkpoint record write the stale version
        // of tx_state. As a result, the log will seems backward.
        // For example, you might see a wal record that rollback a txn
        // followed by a checkpoint record saying the tx_state is Active(TxId).
        let lsn = self
            .wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Rollback { txid: self.id },
            })
            .await?;

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

        undo_txn(&self.pager, &self.wal, self.id, last_undone).await?;
        self.wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::End { txid: self.id },
            })
            .await?;
        *tx_state = TxState::None;

        Ok(())
    }
}

pub struct WriteBucket<'a, R: Runtime> {
    btree: BTree<'a, R>,
}

impl<'a, R: Runtime> WriteBucket<'a, R> {
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.btree.put(key, value).await?;
        Ok(())
    }
}

impl<'a, R: Runtime> Bucket<'a, R> for WriteBucket<'a, R> {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.btree.get(key).await?;
        let Some(result) = result else {
            return Ok(None);
        };
        let value = result.get().await?.value().to_vec();
        Ok(Some(value))
    }

    async fn range<'b, Rg>(&self, range: Rg) -> anyhow::Result<Range<'a, R>>
    where
        'a: 'b,
        Rg: RangeBounds<&'b [u8]>,
    {
        let cursor = self.btree.range(range).await?;
        Ok(Range {
            error: false,
            cursor,
        })
    }
}

pub trait Bucket<'a, R: Runtime> {
    fn get(&self, key: &[u8]) -> impl Future<Output = anyhow::Result<Option<Vec<u8>>>>;

    fn range<'b, Rg>(&self, range: Rg) -> impl Future<Output = anyhow::Result<Range<'a, R>>>
    where
        'a: 'b,
        Rg: RangeBounds<&'b [u8]>;
}

pub struct Range<'a, R: Runtime> {
    error: bool,
    cursor: Cursor<'a, R>,
}

impl<'a, R: Runtime> Range<'a, R> {
    pub async fn next(&mut self) -> Option<anyhow::Result<KeyValue>> {
        if self.error {
            return None;
        }

        match self.cursor.next().await {
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

    _tx_guard: <R::RwMutex<()> as RwMutex<()>>::ReadGuard<'db>,
}

impl<'db, R: Runtime> ReadTx<'db, R> {
    fn new(
        txid: TxId,
        db: &Db<R>,
        tx_guard: <R::RwMutex<()> as RwMutex<()>>::ReadGuard<'db>,
    ) -> anyhow::Result<Self> {
        let tx = Self {
            txid,
            pager: db.pager.clone(),
            wal: db.wal.clone(),
            _tx_guard: tx_guard,
        };
        Ok(tx)
    }

    pub async fn bucket(&self, name: &str) -> anyhow::Result<Option<ReadBucket<R>>> {
        let Some(root_pgid) = self.pager.read_state().await.root else {
            return Ok(None);
        };

        let btree = crate::btree::new(self.txid, &self.pager, &self.wal, root_pgid);
        let result = btree.get(name.as_bytes()).await?;

        let Some(result) = result else {
            return Ok(None);
        };

        let result = result.get().await?;
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

impl<'a, R: Runtime> Bucket<'a, R> for ReadBucket<'a, R> {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.btree.get(key).await?;
        let Some(result) = result else {
            return Ok(None);
        };
        let value = result.get().await?.value().to_vec();
        Ok(Some(value))
    }

    async fn range<'b, Rg>(&self, range: Rg) -> anyhow::Result<Range<'a, R>>
    where
        'a: 'b,
        Rg: RangeBounds<&'b [u8]>,
    {
        let cursor = self.btree.range(range).await?;
        Ok(Range {
            error: false,
            cursor,
        })
    }
}
