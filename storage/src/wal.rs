use crate::id::Lsn;
use crate::log::{WalDecodeResult, WalEntry, WalHeader, WalKind, WAL_HEADER_SIZE};
use crate::metric::{Histogram, HistogramPercentile};
use crate::pager::MAXIMUM_PAGE_SIZE;
use crate::runtime::{File, JoinHandle, Mutex, Runtime, RwMutex, Timer, TimerHandle};
use anyhow::{anyhow, Context};
use std::io::SeekFrom;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const BUFFER_SIZE: usize = MAXIMUM_PAGE_SIZE * 20;

pub(crate) struct Wal<R: Runtime> {
    f1: Option<Arc<R::Mutex<WalFile<R>>>>,
    f2: Option<Arc<R::Mutex<WalFile<R>>>>,
    buffer: Arc<R::RwMutex<Buffer>>,
    internal: Arc<R::RwMutex<WalInternal>>,
    iter_backward_lock: R::Mutex<Vec<u8>>,
    flush_trigger: Option<R::TimerHandle>,
    background: Option<R::JoinHandle>,

    stat: Arc<StatInternal>,
}

impl<R: Runtime> Drop for Wal<R> {
    fn drop(&mut self) {
        self.flush_trigger.take();
        if self.background.is_some() {
            panic!("Wal is dropped without properly shutdown-ed")
        }
    }
}

struct WalFile<R: Runtime> {
    f: R::File,
    relative_lsn: u64,
    is_empty: bool,
    checkpoint: bool,
}

struct Buffer {
    buff: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
}

struct StatInternal {
    bytes_written: AtomicU64,

    flushed_total: AtomicU64,
    flushed_because_buffer_almost_full: AtomicU64,
    flushed_because_buffer_full: AtomicU64,
    flushed_because_manual_trigger: AtomicU64,
    flushed_because_sync_request: AtomicU64,

    flush_latency: Histogram,
}

#[derive(Debug)]
pub(crate) struct Stat {
    pub(crate) bytes_written: u64,

    pub(crate) flushed_total: u64,
    pub(crate) flushed_because_timeout: u64,
    pub(crate) flushed_because_buffer_almost_full: u64,
    pub(crate) flushed_because_buffer_full: u64,
    pub(crate) flushed_because_manual_trigger: u64,
    pub(crate) flushed_because_sync_request: u64,

    pub(crate) flush_latency: HistogramPercentile,
}

impl Buffer {
    #[inline]
    fn len(&self) -> usize {
        if self.start_offset <= self.end_offset {
            self.end_offset - self.start_offset
        } else {
            self.end_offset + self.buff.len() - self.start_offset
        }
    }

    #[inline]
    fn size(&self) -> usize {
        self.buff.len()
    }
}

struct WalInternal {
    temp_buffer: Vec<u8>,
    use_wal_1: bool,

    next: Lsn,
    first_unflushed: Lsn,
}

impl<R: Runtime> Wal<R> {
    async fn new(
        mut f1: WalFile<R>,
        mut f2: WalFile<R>,
        use_wal_1: bool,
        next_lsn: Lsn,
    ) -> anyhow::Result<Self> {
        let f = if use_wal_1 { &mut f1 } else { &mut f2 };
        assert!(next_lsn.get() >= f.relative_lsn);
        let size = next_lsn.get() - f.relative_lsn;
        // WARNING: it is important to perform fsync here to make sure that this file is
        // successfully truncated to the last log entry. Otherwise, we might end up with a logs
        // with old invalid entries. It is possible that the process crashed in the past and some
        // the wal entries are not properly written, so some of them might be valid, and some of
        // them might not be valid. If we don't truncate our wal here, we might end up writing some
        // log entries until it reach some old wal entry that has been there since last crash. If
        // we crash at this point, that particular old wal entry will be considered as valid wal
        // entry because it follows our valid wal entry.
        f.f.truncate(2u64 * WAL_HEADER_SIZE as u64 + size).await?;
        f.f.sync().await?;

        let internal = Arc::new(R::RwMutex::new(WalInternal {
            temp_buffer: vec![0u8; MAXIMUM_PAGE_SIZE],
            use_wal_1,
            next: next_lsn,
            first_unflushed: next_lsn,
        }));
        let f1 = Arc::new(R::Mutex::new(f1));
        let f2 = Arc::new(R::Mutex::new(f2));
        let buffer = Arc::new(R::RwMutex::new(Buffer {
            buff: vec![0u8; BUFFER_SIZE],
            start_offset: 0,
            end_offset: 0,
        }));
        let stat = Arc::new(StatInternal {
            bytes_written: AtomicU64::new(0),
            flushed_total: AtomicU64::new(0),
            flushed_because_buffer_almost_full: AtomicU64::new(0),
            flushed_because_buffer_full: AtomicU64::new(0),
            flushed_because_manual_trigger: AtomicU64::new(0),
            flushed_because_sync_request: AtomicU64::new(0),
            flush_latency: Histogram::new_exponential(1.0, 1.3, 30),
        });

        let (mut timer, timer_handle) = R::timer(std::time::Duration::from_secs(3600));
        let background = {
            let internal = internal.clone();
            let buffer = buffer.clone();
            let f1 = f1.clone();
            let f2 = f2.clone();
            let stat = stat.clone();
            R::spawn("wal_flusher", async move {
                while timer.wait().await {
                    let mut internal = internal.write().await;
                    let mut buffer = buffer.write().await;
                    if let Err(err) = Self::flush(&mut internal, &mut buffer, &f1, &f2, &stat).await
                    {
                        log::error!("wal_flush_error err={err}");
                    }
                }
            })
            .await
        };

        let backward_buffer = vec![0u8; buffer.read().await.size()];
        Ok(Wal {
            f1: Some(f1),
            f2: Some(f2),
            buffer,
            internal,
            flush_trigger: Some(timer_handle),
            iter_backward_lock: Mutex::new(backward_buffer),
            background: Some(background),
            stat,
        })
    }

    pub(crate) async fn complete_checkpoint(&self, checkpoint_lsn: Lsn) -> anyhow::Result<()> {
        self.sync(checkpoint_lsn).await?;

        let mut internal = self.internal.write().await;
        let internal = &mut *internal;

        let f = if internal.use_wal_1 {
            self.f1.as_ref().unwrap()
        } else {
            self.f2.as_ref().unwrap()
        };
        let mut old_f = f.lock().await;

        let header = WalHeader {
            version: 0,
            checkpoint: Some(checkpoint_lsn),
            relative_lsn: old_f.relative_lsn,
        };

        let mut buff = [0u8; WAL_HEADER_SIZE];
        header.encode(&mut buff[..WAL_HEADER_SIZE]);

        // WARNING: it is important that the second block of checkpoint header is written
        // first before the first one because during recovery, we read the first block first.
        // If the first block is already valid, we don't read the second one. But, if the
        // first one is corrupted (or partially written), we should check the second one.
        // In case the write is failed in the second block, we can assume the whole write
        // is incompleted, thus the next recover mechanism will read the first block.
        // In case the write is failed in the first block, we can recover it by reading
        // the second block that guaranteed to be fully written.
        // Of course, if this is the first write (the file was empty before), it might be
        // that both the first and the second block is invalid. In that case, we assume that
        // the wal file is empty.
        old_f
            .f
            .seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))
            .await?;
        old_f.f.write_all(&buff).await?;
        old_f.f.sync().await?;

        old_f.f.seek(SeekFrom::Start(0)).await?;
        old_f.f.write_all(&buff).await?;
        old_f.f.sync().await?;

        self.stat
            .bytes_written
            .fetch_add(buff.len() as u64 * 2, Ordering::SeqCst);

        old_f.is_empty = false;
        old_f.checkpoint = true;

        Ok(())
    }

    async fn end_transaction(
        internal: &mut WalInternal,
        f1: &R::Mutex<WalFile<R>>,
        f2: &R::Mutex<WalFile<R>>,
        stat: &StatInternal,
        buffer: &mut Buffer,
        iter_backward_lock: &R::Mutex<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let backward_iter_guard = iter_backward_lock.try_lock().await;
        assert!(
            backward_iter_guard.is_some(),
            "cannot end transaction while iterating backward because the wal might be swapped and disrupt the backward iteration",
        );

        let (old_f, new_f) = if internal.use_wal_1 {
            (f1, f2)
        } else {
            (f2, f1)
        };

        let mut old_f = old_f.lock().await;
        if !old_f.checkpoint {
            log::debug!("wal_is_not_swapped no checkpoint yet");
            return Ok(());
        }

        Self::flush_internal(internal, buffer, &mut old_f, stat).await?;

        // the new wal file is marked empty so that the next time we flush to it,
        // everything is resetted.
        let mut new_f = new_f.lock().await;
        internal.use_wal_1 = !internal.use_wal_1;
        new_f.is_empty = true;
        new_f.checkpoint = false;
        new_f.relative_lsn = internal.first_unflushed.get();

        Ok(())
    }

    pub(crate) async fn append_log(&self, entry: WalEntry<'_>) -> anyhow::Result<Lsn> {
        let size = entry.size();

        let mut internal = self.internal.write().await;
        let internal = &mut *internal;

        let mut buffer = self.buffer.write().await;
        if buffer.len() + size > buffer.size() {
            self.stat
                .flushed_because_buffer_full
                .fetch_add(1, Ordering::SeqCst);
            Self::flush(
                internal,
                &mut buffer,
                self.f1.as_ref().unwrap(),
                self.f2.as_ref().unwrap(),
                &self.stat,
            )
            .await?;
        }

        let buffer_was_big_enough = buffer.len() > buffer.size() / 2;

        let end_offset = buffer.end_offset;
        if end_offset + size > buffer.size() {
            let part1 = buffer.size() - end_offset;
            let part2 = size - part1;
            entry.encode(&mut internal.temp_buffer[..size]);
            buffer.buff[end_offset..].copy_from_slice(&internal.temp_buffer[..part1]);
            buffer.buff[..part2].copy_from_slice(&internal.temp_buffer[part1..size]);
            buffer.end_offset = part2;
        } else {
            entry.encode(&mut buffer.buff[end_offset..end_offset + size]);
            buffer.end_offset += size;
        }
        let lsn = internal.next;
        internal.next.add_assign(size as u64);

        let buffer_is_big_enough = buffer.len() > buffer.size() / 2;

        if !buffer_was_big_enough && buffer_is_big_enough {
            // TODO: ok, actually we shouldn't trigger this when there is an ongoing wal flushed
            self.stat
                .flushed_because_buffer_almost_full
                .fetch_add(1, Ordering::SeqCst);
            if let Some(ref trigger) = self.flush_trigger {
                trigger.trigger().await;
            }
        }

        if matches!(entry.kind, WalKind::End { .. }) {
            Self::end_transaction(
                internal,
                self.f1.as_ref().unwrap(),
                self.f2.as_ref().unwrap(),
                &self.stat,
                &mut buffer,
                &self.iter_backward_lock,
            )
            .await?;
        }

        log::debug!("wal_appended {lsn:?} {entry:?}");
        Ok(lsn)
    }

    async fn flush(
        internal: &mut WalInternal,
        buffer: &mut Buffer,
        f1: &R::Mutex<WalFile<R>>,
        f2: &R::Mutex<WalFile<R>>,
        stat: &StatInternal,
    ) -> anyhow::Result<()> {
        let f = if internal.use_wal_1 { f1 } else { f2 };
        let mut f = f.lock().await;
        Self::flush_internal(internal, buffer, &mut f, stat).await
    }

    async fn flush_internal(
        internal: &mut WalInternal,
        buffer: &mut Buffer,
        f: &mut WalFile<R>,
        stat: &StatInternal,
    ) -> anyhow::Result<()> {
        log::debug!(
            use_wal_1=internal.use_wal_1,
            len=buffer.len(),
            is_empty=f.is_empty,
            checkpoint=f.checkpoint,
            relative_lsn=f.relative_lsn,
            first_unflushed:?=internal.first_unflushed,
            next:?=internal.next;
            "flushing_wal",
        );

        let start = std::time::Instant::now();

        if f.is_empty {
            let mut buff = [0u8; WAL_HEADER_SIZE * 2];
            let header = WalHeader {
                version: 0,
                checkpoint: None,
                relative_lsn: f.relative_lsn,
            };
            header.encode(&mut buff[..WAL_HEADER_SIZE]);
            header.encode(&mut buff[WAL_HEADER_SIZE..]);

            // WARNING: it is important to perform fsync here to make sure that this file is
            // successfully truncated to zero. Otherwise, we might end up with partially written
            // content where the header is sucessfully persisted, but some of its content are not,
            // and leaving us with combination of the new log entries and the old log entries. To
            // make things worse, if it happen such that there is an old log entry that starts at
            // the position which should be used for the new log entry, we can silently fall into
            // bug and it's hard to find out the first root cause.
            f.f.truncate(0).await?;
            f.f.sync().await?;

            f.f.seek(SeekFrom::Start(0)).await?;
            f.f.write_all(&buff).await?;
            stat.bytes_written
                .fetch_add(buff.len() as u64, Ordering::SeqCst);
            f.is_empty = false;
        }

        let offset = internal.first_unflushed.get() - f.relative_lsn + WAL_HEADER_SIZE as u64 * 2;
        f.f.seek(SeekFrom::Start(offset)).await?;

        let written = if buffer.end_offset < buffer.start_offset {
            f.f.write_all(&buffer.buff[buffer.start_offset..]).await?;
            f.f.write_all(&buffer.buff[..buffer.end_offset]).await?;
            buffer.buff.len() - buffer.start_offset + buffer.end_offset
        } else {
            f.f.write_all(&buffer.buff[buffer.start_offset..buffer.end_offset])
                .await?;
            buffer.end_offset - buffer.start_offset
        };
        stat.bytes_written
            .fetch_add(written as u64, Ordering::SeqCst);
        stat.flushed_total.fetch_add(1, Ordering::SeqCst);

        f.f.sync().await?;

        let elapsed = start.elapsed();
        stat.flush_latency.observe(elapsed.as_millis() as f64);

        buffer.start_offset = 0;
        buffer.end_offset = 0;
        internal.first_unflushed = internal.next;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn trigger_flush(&self) -> anyhow::Result<()> {
        self.stat
            .flushed_because_manual_trigger
            .fetch_add(1, Ordering::SeqCst);
        let mut internal = self.internal.write().await;
        let mut buffer = self.buffer.write().await;
        Self::flush(
            internal.deref_mut(),
            buffer.deref_mut(),
            self.f1.as_ref().unwrap(),
            self.f2.as_ref().unwrap(),
            &self.stat,
        )
        .await
    }

    pub(crate) async fn first_unflushed(&self) -> Lsn {
        self.internal.read().await.first_unflushed
    }

    pub(crate) async fn sync(&self, lsn: Lsn) -> anyhow::Result<Lsn> {
        let internal = self.internal.read().await;
        assert!(lsn < internal.next);
        if internal.first_unflushed > lsn {
            return Ok(internal.first_unflushed);
        }
        drop(internal);

        let mut internal = self.internal.write().await;
        if internal.first_unflushed > lsn {
            return Ok(internal.first_unflushed);
        }

        let mut buffer = self.buffer.write().await;
        self.stat
            .flushed_because_sync_request
            .fetch_add(1, Ordering::SeqCst);
        Self::flush(
            &mut internal,
            &mut buffer,
            self.f1.as_ref().unwrap(),
            self.f2.as_ref().unwrap(),
            &self.stat,
        )
        .await?;
        Ok(internal.first_unflushed)
    }

    pub(crate) async fn iter_back(&self, upper_bound: Lsn) -> anyhow::Result<IterBack<R>> {
        let mut buffer = self.iter_backward_lock.lock().await;

        // Warning: it is important that `internal` is locked before `wal_buffer` because
        // `sync` method might run in different thread that might lock `internal` and `wal_buffer` in the same order.
        let internal = self.internal.read().await;
        let wal_buffer = self.buffer.read().await;
        let buffer_len = buffer.len();

        let filled = if upper_bound > internal.first_unflushed {
            let offset = upper_bound.get() - internal.first_unflushed.get();

            let start = wal_buffer.start_offset;
            let end = wal_buffer.start_offset + offset as usize;

            if end > wal_buffer.size() {
                let to_copy = &wal_buffer.buff[..end % wal_buffer.size()];
                let n1 = to_copy.len();
                buffer[buffer_len - n1..buffer_len].copy_from_slice(to_copy);

                let to_copy = &wal_buffer.buff[start..];
                let n2 = to_copy.len();
                buffer[buffer_len - n1 - n2..buffer_len - n1].copy_from_slice(to_copy);

                n1 + n2
            } else {
                let to_copy = &wal_buffer.buff[start..end];
                buffer[buffer_len - to_copy.len()..].copy_from_slice(to_copy);
                to_copy.len()
            }
        } else {
            0
        };
        drop(wal_buffer);

        // This is ok because:
        // * All log record of a transaction always comes from a single WAL file or a temporary
        //   buffer. We will never swap a wal file if there is an active or aborting transaction.
        // * The database never undo non-last transaction, so the logs must always
        //   comes from the latest wal file.
        // * The wal will never be swapped during `iter_back` because in order to swap the WAL, a
        //   transaction end should be called and it will never happen in the middle of
        //   `iter_back`.
        let f = if internal.use_wal_1 {
            self.f1.as_ref().unwrap()
        } else {
            self.f2.as_ref().unwrap()
        };
        drop(internal);

        // Note that it's possible that some entries might not even on `f`, but on the buffer

        let current_end_lsn = upper_bound;
        let start_offset = buffer_len - filled;
        let end_offset = buffer_len;

        Ok(IterBack {
            buffer,
            buffer_len,
            start_offset,
            end_offset,
            current_end_lsn,
            f: f.clone(),
        })
    }

    pub(crate) async fn shutdown(mut self) -> anyhow::Result<()> {
        self.flush_trigger.take();
        self.background
            .take()
            .expect("background thread should not joined yet")
            .join()
            .await;

        // WARNING: it is important that the background future is
        // joined before self.f1 and self.f2 is closed, because
        // the background thread contains reference to self.f1
        // and self.2. Calling Arc::into_inner would fail if
        // there are more than one owner of the Arc.

        let f1 = self.f1.take().unwrap();
        let f1 = Arc::into_inner(f1).unwrap();
        let f1 = f1.into_inner().await;
        f1.f.close().await?;

        let f2 = self.f2.take().unwrap();
        let f2 = Arc::into_inner(f2).unwrap();
        let f2 = f2.into_inner().await;
        f2.f.close().await?;

        Ok(())
    }

    pub(crate) fn stat(&self) -> Stat {
        let flushed_total = self.stat.flushed_total.load(Ordering::SeqCst);
        let flushed_because_buffer_almost_full = self
            .stat
            .flushed_because_buffer_almost_full
            .load(Ordering::SeqCst);
        let flushed_because_buffer_full =
            self.stat.flushed_because_buffer_full.load(Ordering::SeqCst);
        let flushed_because_manual_trigger = self
            .stat
            .flushed_because_manual_trigger
            .load(Ordering::SeqCst);
        let flushed_because_sync_request = self
            .stat
            .flushed_because_sync_request
            .load(Ordering::SeqCst);

        Stat {
            bytes_written: self.stat.bytes_written.load(Ordering::SeqCst),

            flushed_total,
            flushed_because_timeout: flushed_total
                - flushed_because_buffer_almost_full
                - flushed_because_manual_trigger,
            flushed_because_buffer_almost_full,
            flushed_because_buffer_full,
            flushed_because_manual_trigger,
            flushed_because_sync_request,

            flush_latency: self.stat.flush_latency.percentile(),
        }
    }
}

pub(crate) struct IterBack<'a, R: Runtime> {
    buffer: <R::Mutex<Vec<u8>> as Mutex<Vec<u8>>>::Guard<'a>,
    buffer_len: usize,
    start_offset: usize,
    end_offset: usize,
    current_end_lsn: Lsn,
    f: Arc<R::Mutex<WalFile<R>>>,
}

impl<'a, R: Runtime> IterBack<'a, R> {
    pub(crate) async fn next(&mut self) -> anyhow::Result<Option<(Lsn, WalEntry)>> {
        let entry = WalEntry::decode_backward(&self.buffer[self.start_offset..self.end_offset]);
        // TODO: I'm cheating here. polonius can't come any sooner
        let entry = unsafe { std::mem::transmute(entry) };
        let entry = if let WalDecodeResult::NeedMoreBytes = entry {
            let len = self.end_offset - self.start_offset;
            for i in 0..len {
                self.buffer[self.buffer_len - 1 - i] = self.buffer[self.end_offset - 1 - i];
            }
            self.start_offset = self.buffer_len - len;
            self.end_offset = self.buffer_len;

            let mut guard = self.f.lock().await;
            let f: &mut WalFile<R> = guard.deref_mut();
            if f.is_empty {
                return Ok(None);
            }

            let buffer_remaining = self.start_offset;
            let f_remaining = self.current_end_lsn.get() - f.relative_lsn - len as u64;
            let n_to_read = std::cmp::min(buffer_remaining as u64, f_remaining);
            if n_to_read == 0 {
                return Ok(None);
            }
            let seek_offset = WAL_HEADER_SIZE as u64 * 2 + f_remaining - n_to_read;
            f.f.seek(SeekFrom::Start(seek_offset)).await?;
            f.f.read_exact(
                &mut self.buffer[self.start_offset - n_to_read as usize..self.start_offset],
            )
            .await?;
            self.start_offset -= n_to_read as usize;
            let tmp = WalEntry::decode_backward(&self.buffer[self.start_offset..self.end_offset]);
            // TODO: I'm cheating here. polonius can't come any sooner
            unsafe { std::mem::transmute(tmp) }
        } else {
            entry
        };

        match entry {
            WalDecodeResult::Ok(entry) => {
                let entry_size = entry.size();
                let lsn = self.current_end_lsn.sub(entry_size as u64);
                self.end_offset -= entry_size;
                self.current_end_lsn.sub_assign(entry_size as u64);
                Ok(Some((lsn, entry)))
            }
            WalDecodeResult::NeedMoreBytes | WalDecodeResult::Invalid => {
                return Ok(None);
            }
            WalDecodeResult::Err(err) => return Err(err),
        }
    }
}

pub(crate) async fn recover<R: Runtime>(path: &Path) -> anyhow::Result<Recovering<R>> {
    let wal_path_1 = path.join("wal_1");
    let wal_file_1 = R::File::open(&wal_path_1).await?;
    if !wal_file_1.is_file().await? {
        return Err(anyhow!("{wal_path_1:?} is not a regular file"));
    }
    let f1 = recover_wal_file::<R>(wal_file_1)
        .await
        .with_context(|| format!("cannot init wal file {wal_path_1:?}"))?;

    let wal_path_2 = path.join("wal_2");
    let wal_file_2 = R::File::open(&wal_path_2).await?;
    if !wal_file_2.is_file().await? {
        return Err(anyhow!("{wal_path_2:?} is not a regular file"));
    }
    let f2 = recover_wal_file::<R>(wal_file_2)
        .await
        .with_context(|| format!("cannot init wal file {wal_path_2:?}"))?;

    let (use_wal_1, checkpoint) = match (f1.checkpoint, f2.checkpoint) {
        (Some(f1_lsn), Some(f2_lsn)) => {
            if f1_lsn >= f2_lsn {
                (true, f1_lsn)
            } else {
                (false, f2_lsn)
            }
        }
        (Some(f1_lsn), None) => (true, f1_lsn),
        (None, Some(f2_lsn)) => (false, f2_lsn),
        (None, None) => (true, Lsn::new(0)),
    };

    log::debug!(f1:?,f2:?,use_wal_1,checkpoint:?; "recovering");

    let buffer = vec![0u8; BUFFER_SIZE];
    let next_lsn = checkpoint;
    let start_offset = 0;
    let end_offset = 0;
    let current_lsn = checkpoint;

    Ok(Recovering {
        buffer,
        start_offset,
        end_offset,
        use_wal_1,
        f1,
        f2,
        current_lsn,
        next_lsn,
    })
}

pub(crate) struct Recovering<R: Runtime> {
    buffer: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
    use_wal_1: bool,
    f1: RecoveringWalFile<R>,
    f2: RecoveringWalFile<R>,
    current_lsn: Lsn,
    next_lsn: Lsn,
}

impl<R: Runtime> Recovering<R> {
    pub(crate) async fn next(&mut self) -> anyhow::Result<Option<(Lsn, WalEntry)>> {
        let buffer: &'static mut [u8] = unsafe { std::mem::transmute(&mut *self.buffer) };

        let buff = &buffer[self.start_offset..self.end_offset];
        let entry = WalEntry::decode(buff);

        if let WalDecodeResult::NeedMoreBytes = entry {
            let f = if self.use_wal_1 {
                &mut self.f1
            } else {
                &mut self.f2
            };

            let len = self.end_offset - self.start_offset;
            for i in 0..len {
                buffer[i] = buffer[self.start_offset + i];
            }
            self.start_offset = 0;
            self.end_offset = len;

            if f.is_empty {
                return Ok(None);
            }
            f.f.seek(SeekFrom::Start(
                self.current_lsn.get() - f.relative_lsn + WAL_HEADER_SIZE as u64 * 2 + len as u64,
            ))
            .await?;
            let n = f.f.read(&mut buffer[self.end_offset..]).await?;
            if n == 0 {
                let next_f = if self.use_wal_1 { &self.f2 } else { &self.f1 };
                if next_f.relative_lsn < self.current_lsn.get() {
                    return Ok(None);
                }
                self.use_wal_1 = !self.use_wal_1;
                self.start_offset = 0;
                self.end_offset = 0;
                return self.next().await;
            }

            self.end_offset += n;
        }

        let entry = WalEntry::decode(&buffer[self.start_offset..self.end_offset]);

        match entry {
            WalDecodeResult::Ok(entry) => {
                let lsn = self.current_lsn;
                let entry_size = entry.size();
                self.start_offset += entry_size;
                self.current_lsn.add_assign(entry_size as u64);
                self.next_lsn = self.current_lsn;
                return Ok(Some((lsn, entry)));
            }
            WalDecodeResult::NeedMoreBytes => (),
            WalDecodeResult::Invalid => {
                return Ok(None);
            }
            WalDecodeResult::Err(err) => return Err(err),
        }

        let next_f = if self.use_wal_1 { &self.f2 } else { &self.f1 };
        if next_f.relative_lsn < self.current_lsn.get() {
            return Ok(None);
        }
        self.use_wal_1 = !self.use_wal_1;
        self.start_offset = 0;
        self.end_offset = 0;
        drop(entry);
        return self.next().await;
    }

    pub(crate) async fn finish(self) -> anyhow::Result<Wal<R>> {
        Wal::new(
            self.f1.into(),
            self.f2.into(),
            self.use_wal_1,
            self.next_lsn,
        )
        .await
    }
}

async fn recover_wal_file<R: Runtime>(mut f: R::File) -> anyhow::Result<RecoveringWalFile<R>> {
    let file_size = f.len().await?;
    if file_size < WAL_HEADER_SIZE as u64 * 2 {
        return Ok(RecoveringWalFile {
            f,
            relative_lsn: 0,
            checkpoint: None,
            is_empty: true,
        });
    }

    let mut buff = [0u8; WAL_HEADER_SIZE * 2];
    f.seek(SeekFrom::Start(0)).await?;
    f.read_exact(&mut buff).await?;

    let header = if let Some(header) = WalHeader::decode(&buff[..WAL_HEADER_SIZE]) {
        header
    } else if let Some(header) = WalHeader::decode(&buff[WAL_HEADER_SIZE..]) {
        // When the first part of the wal header is corrupted, but the second part is valid, it
        // means the process was crashed in the past when writing the wal header. In that case, we
        // should repair the first part of the wal header. If we don't recover the first part, when
        // we update the wal header again in the future, and crashed in the middle of writing the
        // second part of the wal header, our wal header become fully corrupted, because the first
        // part is corrupted due to previous sync, and the second part is corrupted due to the next
        // sync.
        f.seek(SeekFrom::Start(0)).await?;
        f.write_all(&buff[WAL_HEADER_SIZE..]).await?;
        f.sync().await?;

        header
    } else {
        // if the first and second part of the wal header is invalid, this means
        // that the wal file is not written successfully, and we can assume that
        // it never been written just like an empty wal.
        return Ok(RecoveringWalFile {
            f,
            relative_lsn: 0,
            checkpoint: None,
            is_empty: true,
        });
    };

    log::info!("wal_header_decoded header={header:?}");

    if header.version != 0 {
        return Err(anyhow!("only wal version 0 is supported"));
    }

    Ok(RecoveringWalFile {
        f,
        relative_lsn: header.relative_lsn,
        checkpoint: header.checkpoint,
        is_empty: false,
    })
}

struct RecoveringWalFile<R: Runtime> {
    f: R::File,
    relative_lsn: u64,
    checkpoint: Option<Lsn>,
    is_empty: bool,
}

impl<R: Runtime> From<RecoveringWalFile<R>> for WalFile<R> {
    fn from(value: RecoveringWalFile<R>) -> Self {
        Self {
            f: value.f,
            relative_lsn: value.relative_lsn,
            is_empty: value.is_empty,
            checkpoint: value.checkpoint.is_some(),
        }
    }
}

impl<R: Runtime> std::fmt::Debug for RecoveringWalFile<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecoveringWalFile{{relative_lsn={},checkpoint={:?},is_empty={}}}",
            self.relative_lsn, self.checkpoint, self.is_empty
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{PageId, TxId};
    use crate::log::TxState;
    use crate::tokio::TokioRuntime;
    use rand::Rng;

    #[tokio::test]
    async fn test_flushing() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        let mut last_seen_lsn = None;
        for i in 1..=10 {
            let lsn = wal
                .append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
            if let Some(last_seen_lsn) = last_seen_lsn {
                assert!(
                    lsn > last_seen_lsn,
                    "last_seen_lsn={last_seen_lsn:?} lsn={lsn:?}"
                );
            }
            last_seen_lsn = Some(lsn);
        }
        wal.shutdown().await?;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {
            panic!("since the wal is not flushed yet, there should be no entry")
        }
        let wal = wal.finish().await?;

        let mut last_seen_lsn = None;
        for i in 1..=10 {
            let lsn = wal
                .append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
            if let Some(last_seen_lsn) = last_seen_lsn {
                assert!(
                    lsn > last_seen_lsn,
                    "last_seen_lsn={last_seen_lsn:?} lsn={lsn:?}"
                );
            }
            last_seen_lsn = Some(lsn);
        }
        wal.trigger_flush().await?;
        wal.shutdown().await?;

        let mut i = 1;
        let mut last_seen_lsn = None;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            if let Some(last_seen_lsn) = last_seen_lsn {
                assert!(
                    lsn > last_seen_lsn,
                    "last_seen_lsn={last_seen_lsn:?} lsn={lsn:?}"
                );
            }
            last_seen_lsn = Some(lsn);
            assert!(entry.clr.is_none());
            let WalKind::LeafInit { txid, pgid } = entry.kind else {
                panic!("the entry should be a leaf init");
            };
            assert_eq!(TxId::new(i).unwrap(), txid);
            assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
            i += 1;
        }
        let wal = wal.finish().await?;

        assert_eq!(11, i);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_checkpoint() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        for i in 1..=10 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        let checkpoint_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: TxState::None,
                    root: PageId::new(123),
                    freelist: PageId::new(321),
                    page_count: 99,
                },
            })
            .await?;
        for i in 11..=15 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        for i in 16..=20 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.shutdown().await?;

        let mut i = 11;
        let mut checkpoint_consumed = false;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            assert!(entry.clr.is_none());

            if matches!(entry.kind, WalKind::End { .. }) {
                return Ok(());
            }

            if !checkpoint_consumed {
                assert_eq!(checkpoint_lsn, lsn);
                let WalKind::Checkpoint {
                    ref active_tx,
                    root,
                    freelist,
                    page_count,
                } = entry.kind
                else {
                    panic!("the entry should be a checkpoint");
                };
                assert_eq!(TxState::None, *active_tx);
                assert_eq!(PageId::new(123), root);
                assert_eq!(PageId::new(321), freelist);
                assert_eq!(99u64, page_count);
                checkpoint_consumed = true;
            } else {
                let WalKind::LeafInit { txid, pgid } = entry.kind else {
                    panic!("the entry should be a leaf init");
                };
                assert_eq!(TxId::new(i).unwrap(), txid);
                assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
                i += 1;
            }
        }
        let wal = wal.finish().await?;

        assert_eq!(16, i);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_recovering_from_wal_1_and_2() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        for i in 1..=10 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        let checkpoint_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: TxState::None,
                    root: PageId::new(123),
                    freelist: PageId::new(321),
                    page_count: 99,
                },
            })
            .await?;
        for i in 11..=15 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.trigger_flush().await?;
        for i in 16..=20 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        for i in 21..=25 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.trigger_flush().await?;
        for i in 26..=30 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.shutdown().await?;

        let mut i = 11;
        let mut checkpoint_consumed = false;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            assert!(entry.clr.is_none());

            if matches!(entry.kind, WalKind::End { .. }) {
                continue;
            }

            if !checkpoint_consumed {
                assert_eq!(checkpoint_lsn, lsn);
                let WalKind::Checkpoint {
                    ref active_tx,
                    root,
                    freelist,
                    page_count,
                } = entry.kind
                else {
                    panic!("the entry should be a checkpoint");
                };
                assert_eq!(TxState::None, *active_tx);
                assert_eq!(PageId::new(123), root);
                assert_eq!(PageId::new(321), freelist);
                assert_eq!(99u64, page_count);
                checkpoint_consumed = true;
            } else {
                let WalKind::LeafInit { txid, pgid } = entry.kind else {
                    panic!("the entry should be a leaf init");
                };
                assert_eq!(TxId::new(i).unwrap(), txid);
                assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
                i += 1;
            }
        }
        let wal = wal.finish().await?;

        assert_eq!(26, i);

        for i in 26..=30 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        let checkpoint_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: TxState::None,
                    root: None,
                    freelist: None,
                    page_count: 99,
                },
            })
            .await?;
        for i in 31..=35 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        for i in 36..=40 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.trigger_flush().await?;
        for i in 41..=45 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })
            .await?;
        }
        wal.shutdown().await?;

        let mut i = 31;
        let mut checkpoint_consumed = false;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            assert!(entry.clr.is_none());

            if matches!(entry.kind, WalKind::End { .. }) {
                continue;
            }

            if !checkpoint_consumed {
                assert_eq!(checkpoint_lsn, lsn);
                let WalKind::Checkpoint {
                    ref active_tx,
                    root,
                    freelist,
                    page_count,
                } = entry.kind
                else {
                    panic!("the entry should be a checkpoint");
                };
                assert_eq!(TxState::None, *active_tx);
                assert_eq!(None, root);
                assert_eq!(None, freelist);
                assert_eq!(99u64, page_count);
                checkpoint_consumed = true;
            } else {
                let WalKind::LeafInit { txid, pgid } = entry.kind else {
                    panic!("the entry should be a leaf init");
                };
                assert_eq!(TxId::new(i).unwrap(), txid);
                assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
                i += 1;
            }
        }
        let wal = wal.finish().await?;

        assert_eq!(41, i);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_wal_1_and_2_have_checkpoint() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let dummy_entry = |i: u64| WalEntry {
            clr: None,
            kind: WalKind::LeafInit {
                txid: TxId::new(i).unwrap(),
                pgid: PageId::new(1000 + i).unwrap(),
            },
        };
        let checkpoint_entry = || WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: TxState::None,
                root: None,
                freelist: None,
                page_count: 99,
            },
        };

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        for i in 1..=5 {
            wal.append_log(dummy_entry(i)).await?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry()).await?;
        for i in 6..=10 {
            wal.append_log(dummy_entry(i)).await?;
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        for i in 11..=15 {
            wal.append_log(dummy_entry(i)).await?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry()).await?;
        for i in 16..=20 {
            wal.append_log(dummy_entry(i)).await?;
        }
        wal.trigger_flush().await?;
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        wal.shutdown().await?;

        let mut i = 16;
        let mut checkpoint_consumed = false;
        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            assert!(entry.clr.is_none());

            if matches!(entry.kind, WalKind::End { .. }) {
                continue;
            }

            if !checkpoint_consumed {
                assert_eq!(checkpoint_lsn, lsn);
                let WalKind::Checkpoint {
                    ref active_tx,
                    root,
                    freelist,
                    page_count,
                } = entry.kind
                else {
                    panic!("the entry should be a checkpoint");
                };
                assert_eq!(TxState::None, *active_tx);
                assert_eq!(None, root);
                assert_eq!(None, freelist);
                assert_eq!(99u64, page_count);
                checkpoint_consumed = true;
            } else {
                let WalKind::LeafInit { txid, pgid } = entry.kind else {
                    panic!("the entry should be a leaf init");
                };
                assert_eq!(TxId::new(i).unwrap(), txid);
                assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
                i += 1;
            }
        }
        let wal = wal.finish().await?;

        assert_eq!(21, i);

        for i in 21..=25 {
            wal.append_log(dummy_entry(i)).await?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry()).await?;
        for i in 26..=30 {
            wal.append_log(dummy_entry(i)).await?;
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;
        for i in 31..=35 {
            wal.append_log(dummy_entry(i)).await?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry()).await?;
        for i in 36..=40 {
            wal.append_log(dummy_entry(i)).await?;
        }
        wal.trigger_flush().await?;
        wal.complete_checkpoint(checkpoint_lsn).await?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })
        .await?;

        wal.shutdown().await?;

        let mut i = 36;
        let mut checkpoint_consumed = false;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some((lsn, entry)) = wal.next().await? {
            assert!(entry.clr.is_none());

            if matches!(entry.kind, WalKind::End { .. }) {
                continue;
            }

            if !checkpoint_consumed {
                assert_eq!(checkpoint_lsn, lsn);
                let WalKind::Checkpoint {
                    ref active_tx,
                    root,
                    freelist,
                    page_count,
                } = entry.kind
                else {
                    panic!("the entry should be a checkpoint");
                };
                assert_eq!(TxState::None, *active_tx);
                assert_eq!(None, root);
                assert_eq!(None, freelist);
                assert_eq!(99u64, page_count);
                checkpoint_consumed = true;
            } else {
                let WalKind::LeafInit { txid, pgid } = entry.kind else {
                    panic!("the entry should be a leaf init");
                };
                assert_eq!(TxId::new(i).unwrap(), txid);
                assert_eq!(PageId::new(1000 + i).unwrap(), pgid);
                i += 1;
            }
        }
        let wal = wal.finish().await?;

        assert_eq!(41, i);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_buffer_full() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let entry = WalEntry {
            clr: None,
            kind: WalKind::Begin {
                txid: TxId::new(1).unwrap(),
            },
        };
        assert!(BUFFER_SIZE % entry.size() == 0);
        let n = BUFFER_SIZE as u64 / entry.size() as u64;

        {
            let mut wal = recover::<TokioRuntime>(dir.path()).await?;
            while let Some(_) = wal.next().await? {}
            let wal = wal.finish().await?;

            for i in 0u64..3 * n {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::Begin {
                        txid: TxId::new(i + 1).unwrap(),
                    },
                })
                .await?;
            }
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafDeleteForUndo {
                    txid: TxId::new(99999999999999999).unwrap(),
                    pgid: PageId::new(99999999999999999).unwrap(),
                    index: 10,
                },
            })
            .await?;
            wal.trigger_flush().await?;
            wal.shutdown().await?;
        }

        let mut i = 0;

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {
            if i < 3 * n {
                assert_eq!(
                    WalKind::Begin {
                        txid: TxId::new(i + 1).unwrap()
                    },
                    entry.kind
                );
            } else {
                assert_eq!(
                    WalKind::LeafDeleteForUndo {
                        txid: TxId::new(99999999999999999).unwrap(),
                        pgid: PageId::new(99999999999999999).unwrap(),
                        index: 10,
                    },
                    entry.kind
                );
            }
            i += 1;
        }
        let wal = wal.finish().await?;

        assert_eq!(3 * n + 1, i);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_million_logs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let mut r = rand::thread_rng();
        let mut i = 1;
        let mut last_lsn = Lsn::new(0);
        let mut last_checkpoint = None;

        for _ in 0..100 {
            let mut checkpoint_consumed = false;

            let mut wal = recover::<TokioRuntime>(dir.path()).await?;
            while let Some((lsn, entry)) = wal.next().await? {
                assert!(lsn >= last_lsn);
                last_lsn = lsn;
                assert!(entry.clr.is_none());

                if matches!(entry.kind, WalKind::End { .. }) {
                    continue;
                }

                if !checkpoint_consumed {
                    assert_eq!(last_checkpoint.unwrap(), lsn);
                    let WalKind::Checkpoint {
                        ref active_tx,
                        root,
                        freelist,
                        page_count,
                    } = entry.kind
                    else {
                        panic!(
                            "the entry should be a checkpoint, but found a {:?}",
                            entry.kind
                        );
                    };
                    assert_eq!(TxState::None, *active_tx);
                    assert_eq!(None, root);
                    assert_eq!(None, freelist);
                    assert_eq!(99u64, page_count);
                    checkpoint_consumed = true;
                } else {
                    let WalKind::LeafInit { .. } = entry.kind else {
                        panic!("the entry should be a leaf init");
                    };
                }
            }
            let wal = wal.finish().await?;

            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
                i += 1;
            }
            wal.trigger_flush().await?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
                i += 1;
            }
            let checkpoint_lsn = wal
                .append_log(WalEntry {
                    clr: None,
                    kind: WalKind::Checkpoint {
                        active_tx: TxState::None,
                        root: None,
                        freelist: None,
                        page_count: 99,
                    },
                })
                .await?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
                i += 1;
            }
            wal.complete_checkpoint(checkpoint_lsn).await?;
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::End {
                    txid: TxId::new(1).unwrap(),
                },
            })
            .await?;
            last_checkpoint = Some(checkpoint_lsn);
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
                i += 1;
            }
            wal.trigger_flush().await?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })
                .await?;
                i += 1;
            }
            wal.shutdown().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_iterate_backward() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let dummy_entry = |i: u64| WalEntry {
            clr: None,
            kind: WalKind::LeafInit {
                txid: TxId::new(i).unwrap(),
                pgid: PageId::new(1000 + i).unwrap(),
            },
        };
        let checkpoint_entry = || WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: TxState::None,
                root: None,
                freelist: None,
                page_count: 99,
            },
        };

        let mut entries = vec![];

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        for i in 1..=5 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone()).await?;
            entries.push((lsn, entry));
        }
        let entry = checkpoint_entry();
        let checkpoint_lsn = wal.append_log(entry.clone()).await?;
        entries.push((checkpoint_lsn, entry));
        for i in 6..=10 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone()).await?;
            entries.push((lsn, entry));
        }
        wal.complete_checkpoint(checkpoint_lsn).await?;
        let rollback_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Rollback {
                    txid: TxId::new(1).unwrap(),
                },
            })
            .await?;

        let mut iter = wal.iter_back(rollback_lsn).await?;
        loop {
            let item = iter.next().await?;
            let Some((lsn, entry)) = item else {
                break;
            };
            let Some((expected_lsn, expected_entry)) = entries.pop() else {
                break;
            };
            assert_eq!(expected_lsn, lsn);
            assert_eq!(expected_entry, entry);

            wal.append_log(WalEntry {
                clr: Some(lsn),
                kind: WalKind::LeafResetForUndo {
                    txid: TxId::new(1).unwrap(),
                    pgid: PageId::new(1000).unwrap(),
                },
            })
            .await?;
        }
        assert!(entries.is_empty());

        drop(iter);
        wal.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_iterate_backward_from_buffer() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let dummy_entry = |i: u64| WalEntry {
            clr: None,
            kind: WalKind::LeafInit {
                txid: TxId::new(i).unwrap(),
                pgid: PageId::new(1000 + i).unwrap(),
            },
        };

        let mut entries = vec![];

        let mut wal = recover::<TokioRuntime>(dir.path()).await?;
        while let Some(_) = wal.next().await? {}
        let wal = wal.finish().await?;

        for i in 1..=5 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone()).await?;
            entries.push((lsn, entry));
        }
        wal.trigger_flush().await?;
        for i in 6..=10 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone()).await?;
            entries.push((lsn, entry));
        }
        let rollback_lsn = wal
            .append_log(WalEntry {
                clr: None,
                kind: WalKind::Rollback {
                    txid: TxId::new(1).unwrap(),
                },
            })
            .await?;

        let mut iter = wal.iter_back(rollback_lsn).await?;
        loop {
            let item = iter.next().await?;
            let Some((lsn, entry)) = item else {
                break;
            };
            let Some((expected_lsn, expected_entry)) = entries.pop() else {
                break;
            };
            assert_eq!(expected_lsn, lsn);
            assert_eq!(expected_entry, entry);

            wal.append_log(WalEntry {
                clr: Some(lsn),
                kind: WalKind::LeafResetForUndo {
                    txid: TxId::new(1).unwrap(),
                    pgid: PageId::new(1000).unwrap(),
                },
            })
            .await?;
        }
        assert!(entries.is_empty());

        drop(iter);
        wal.shutdown().await?;

        Ok(())
    }

    #[test]
    fn test_incomplete_entry() -> anyhow::Result<()> {
        // test if the last entry is not completed.
        // case 1: the bytes are there, but the checksum is wrong
        // case 2: the bytes are not there
        // might need to mock the file system
        // todo!();
        Ok(())
    }
}
