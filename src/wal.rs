use crate::file_lock::FileLock;
use crate::id::Lsn;
use crate::log::{WalDecodeResult, WalEntry, WalHeader, WalKind, WAL_HEADER_SIZE};
use crate::pager::MAXIMUM_PAGE_SIZE;
use anyhow::{anyhow, Context};
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread::spawn;

const BUFFER_SIZE: usize = MAXIMUM_PAGE_SIZE * 20;

pub(crate) struct Wal {
    f1: Arc<Mutex<WalFile>>,
    f2: Arc<Mutex<WalFile>>,
    buffer: Arc<RwLock<Buffer>>,
    internal: Arc<RwLock<WalInternal>>,
    flush_trigger: SyncSender<()>,
    iter_backward_lock: Mutex<Vec<u8>>,
}

struct WalFile {
    f: File,
    relative_lsn: u64,
    is_empty: bool,
    checkpoint: bool,
}

struct Buffer {
    buff: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
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

impl Wal {
    fn new(f1: WalFile, f2: WalFile, use_wal_1: bool, next_lsn: Lsn) -> Self {
        let internal = Arc::new(RwLock::new(WalInternal {
            temp_buffer: vec![0u8; MAXIMUM_PAGE_SIZE],
            use_wal_1,
            next: next_lsn,
            first_unflushed: next_lsn,
        }));
        let f1 = Arc::new(Mutex::new(f1));
        let f2 = Arc::new(Mutex::new(f2));
        let buffer = Arc::new(RwLock::new(Buffer {
            buff: vec![0u8; BUFFER_SIZE],
            start_offset: 0,
            end_offset: 0,
        }));

        let (flush_trigger, flush_signal) = std::sync::mpsc::sync_channel::<()>(0);
        {
            let internal = internal.clone();
            let buffer = buffer.clone();
            let f1 = f1.clone();
            let f2 = f2.clone();
            spawn(move || loop {
                let result = flush_signal.recv_timeout(std::time::Duration::from_secs(3600));
                if result == Err(RecvTimeoutError::Disconnected) {
                    return;
                }
                let mut internal = internal.write();
                let mut buffer = buffer.write();
                if let Err(err) = Wal::flush(&mut internal, &mut buffer, &f1, &f2) {
                    log::error!("wal_flush_error err={err}");
                }
            });
        }

        let backward_buffer = vec![0u8; buffer.read().size()];
        Wal {
            f1,
            f2,
            buffer,
            internal,
            flush_trigger,
            iter_backward_lock: Mutex::new(backward_buffer),
        }
    }

    pub(crate) fn complete_checkpoint(&self, checkpoint_lsn: Lsn) -> anyhow::Result<()> {
        self.sync(checkpoint_lsn)?;

        let mut internal = self.internal.write();
        let internal = &mut *internal;

        let f = if internal.use_wal_1 {
            &self.f1
        } else {
            &self.f2
        };
        let mut old_f = f.lock();

        let header = WalHeader {
            version: 0,
            checkpoint: Some(checkpoint_lsn),
            relative_lsn: old_f.relative_lsn,
        };

        let mut buff = [0u8; WAL_HEADER_SIZE * 2];
        header.encode(&mut buff[..WAL_HEADER_SIZE]);
        header.encode(&mut buff[WAL_HEADER_SIZE..]);
        old_f.f.seek(SeekFrom::Start(0))?;
        old_f.f.write_all(&buff)?;
        old_f.f.sync_all()?;
        old_f.is_empty = false;
        old_f.checkpoint = true;

        Ok(())
    }

    fn end_transaction(
        internal: &mut WalInternal,
        f1: &Mutex<WalFile>,
        f2: &Mutex<WalFile>,
        buffer: &mut Buffer,
        iter_backward_lock: &Mutex<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let backward_iter_guard = iter_backward_lock.try_lock();
        assert!(
            backward_iter_guard.is_some(),
            "cannot end transaction while iterating backward because the wal might be swapped and disrupt the backward iteration",
        );

        let (old_f, new_f) = if internal.use_wal_1 {
            (f1, f2)
        } else {
            (f2, f1)
        };

        let mut old_f = old_f.lock();
        if !old_f.checkpoint {
            log::debug!("wal_is_not_swapped no checkpoint yet");
            return Ok(());
        }

        Self::flush_internal(internal, buffer, &mut old_f)?;

        // the new wal file is marked empty so that the next time we flush to it,
        // everything is resetted.
        let mut new_f = new_f.lock();
        internal.use_wal_1 = !internal.use_wal_1;
        new_f.is_empty = true;
        new_f.checkpoint = false;
        new_f.relative_lsn = internal.first_unflushed.get();

        Ok(())
    }

    pub(crate) fn append_log(&self, entry: WalEntry<'_>) -> anyhow::Result<Lsn> {
        let size = entry.size();

        let mut internal = self.internal.write();
        let internal = &mut *internal;

        let mut buffer = self.buffer.write();
        if buffer.len() + size > buffer.size() {
            Self::flush(internal, &mut buffer, &self.f1, &self.f2)?;
        }

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

        if buffer.len() > buffer.size() / 4 {
            let _ = self.flush_trigger.try_send(());
        }

        if matches!(entry.kind, WalKind::End { .. }) {
            Self::end_transaction(
                internal,
                &self.f1,
                &self.f2,
                &mut buffer,
                &self.iter_backward_lock,
            )?;
        }

        log::debug!("wal_appended {lsn:?} {entry:?}");
        Ok(lsn)
    }

    fn flush(
        internal: &mut WalInternal,
        buffer: &mut Buffer,
        f1: &Mutex<WalFile>,
        f2: &Mutex<WalFile>,
    ) -> anyhow::Result<()> {
        let f = if internal.use_wal_1 { f1 } else { f2 };
        let mut f = f.lock();
        Self::flush_internal(internal, buffer, &mut f)
    }

    fn flush_internal(
        internal: &mut WalInternal,
        buffer: &mut Buffer,
        f: &mut WalFile,
    ) -> anyhow::Result<()> {
        log::debug!(
            "flushing_wal use_wal_1={} len={} is_empty={} checkpoint-{} relative_lsn={} first_unflushed={:?} next={:?}",
            internal.use_wal_1,
            buffer.len(),
            f.is_empty,
            f.checkpoint,
            f.relative_lsn,
            internal.first_unflushed,
            internal.next,
        );

        if f.is_empty {
            let mut buff = [0u8; WAL_HEADER_SIZE * 2];
            let header = WalHeader {
                version: 0,
                checkpoint: None,
                relative_lsn: f.relative_lsn,
            };
            header.encode(&mut buff[..WAL_HEADER_SIZE]);
            header.encode(&mut buff[WAL_HEADER_SIZE..]);
            f.f.set_len(0)?;
            f.f.seek(SeekFrom::Start(0))?;
            f.f.write_all(&buff)?;
            f.is_empty = false;
        }

        let offset = internal.first_unflushed.get() - f.relative_lsn + WAL_HEADER_SIZE as u64 * 2;
        f.f.seek(SeekFrom::Start(offset))?;

        if buffer.end_offset < buffer.start_offset {
            f.f.write_all(&buffer.buff[buffer.start_offset..])?;
            f.f.write_all(&buffer.buff[..buffer.end_offset])?;
        } else {
            f.f.write_all(&buffer.buff[buffer.start_offset..buffer.end_offset])?;
        }
        f.f.sync_all()?;

        buffer.start_offset = buffer.end_offset;
        internal.first_unflushed = internal.next;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn trigger_flush(&self) -> anyhow::Result<()> {
        Wal::flush(
            &mut self.internal.write(),
            &mut self.buffer.write(),
            &self.f1,
            &self.f2,
        )
    }

    pub(crate) fn first_unflushed(&self) -> Lsn {
        self.internal.read().first_unflushed
    }

    pub(crate) fn sync(&self, lsn: Lsn) -> anyhow::Result<Lsn> {
        let internal = self.internal.read();
        assert!(lsn < internal.next);
        if internal.first_unflushed > lsn {
            return Ok(internal.first_unflushed);
        }
        drop(internal);

        let mut internal = self.internal.write();
        if internal.first_unflushed > lsn {
            return Ok(internal.first_unflushed);
        }

        let mut buffer = self.buffer.write();
        Self::flush(&mut internal, &mut buffer, &self.f1, &self.f2)?;
        Ok(internal.first_unflushed)
    }

    pub(crate) fn iter_back<F>(&self, upper_bound: Lsn, mut handler: F) -> anyhow::Result<()>
    where
        F: FnMut(Lsn, WalEntry) -> anyhow::Result<bool>,
    {
        let mut buffer = self.iter_backward_lock.lock();

        let wal_buffer = self.buffer.read();
        let internal = self.internal.read();
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
            &self.f1
        } else {
            &self.f2
        };
        drop(internal);

        // Note that it's possible that some entries might not even on `f`, but on the buffer

        let mut current_end_lsn = upper_bound;
        let mut start_offset = buffer_len - filled;
        let mut end_offset = buffer_len;

        loop {
            let mut entry = WalEntry::decode_backward(&buffer[start_offset..end_offset]);
            if let WalDecodeResult::NeedMoreBytes = entry {
                let len = end_offset - start_offset;
                for i in 0..len {
                    buffer[buffer_len - 1 - i] = buffer[end_offset - 1 - i];
                }
                start_offset = buffer_len - len;
                end_offset = buffer_len;

                let f: &mut WalFile = &mut f.lock();
                if f.is_empty {
                    break;
                }

                let buffer_remaining = start_offset;
                let f_remaining = current_end_lsn.get() - f.relative_lsn - len as u64;
                let n_to_read = std::cmp::min(buffer_remaining as u64, f_remaining);
                if n_to_read == 0 {
                    break;
                }
                let seek_offset = WAL_HEADER_SIZE as u64 * 2 + f_remaining - n_to_read;
                f.f.seek(SeekFrom::Start(seek_offset))?;
                f.f.read_exact(&mut buffer[start_offset - n_to_read as usize..start_offset])?;
                start_offset -= n_to_read as usize;
                entry = WalEntry::decode_backward(&buffer[start_offset..end_offset]);
            }

            match entry {
                WalDecodeResult::Ok(entry) => {
                    let entry_size = entry.size();
                    let lsn = current_end_lsn.sub(entry_size as u64);
                    end_offset -= entry_size;
                    current_end_lsn.sub_assign(entry_size as u64);
                    if handler(lsn, entry)? {
                        break;
                    }
                }
                WalDecodeResult::NeedMoreBytes | WalDecodeResult::Incomplete => {
                    break;
                }
                WalDecodeResult::Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    pub(crate) fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn recover<F>(path: &Path, mut handler: F) -> anyhow::Result<Wal>
where
    F: FnMut(Lsn, WalEntry) -> anyhow::Result<()>,
{
    let wal_path_1 = path.join("wal_1");
    let wal_file_1 = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&wal_path_1)?
        .lock()?;
    if !wal_file_1.metadata()?.is_file() {
        return Err(anyhow!("{wal_path_1:?} is not a regular file"));
    }
    let mut f1 = recover_wal_file(wal_file_1).context("cannot init wal file {wal_path_1:?}")?;

    let wal_path_2 = path.join("wal_2");
    let wal_file_2 = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&wal_path_2)?
        .lock()?;
    if !wal_file_2.metadata()?.is_file() {
        return Err(anyhow!("{wal_path_2:?} is not a regular file"));
    }
    let mut f2 = recover_wal_file(wal_file_2).context("cannot init wal file {wal_path_2:?}")?;

    let (mut use_wal_1, checkpoint) = match (f1.checkpoint, f2.checkpoint) {
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

    log::debug!(
        "recovering f1={f1:?} f2={f2:?} start_with_1={use_wal_1} checkpoint={checkpoint:?}"
    );

    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut next_lsn = checkpoint;
    {
        let mut start_offset = 0;
        let mut end_offset = 0;
        let mut current_lsn = checkpoint;
        loop {
            let buff = &buffer[start_offset..end_offset];
            let mut entry = WalEntry::decode(buff);

            let f = if use_wal_1 { &mut f1 } else { &mut f2 };

            if let WalDecodeResult::NeedMoreBytes = entry {
                let len = end_offset - start_offset;
                for i in 0..len {
                    buffer[i] = buffer[start_offset + i];
                }
                start_offset = 0;
                end_offset = len;

                if f.is_empty {
                    break;
                }
                f.f.seek(SeekFrom::Start(
                    current_lsn.get() - f.relative_lsn + WAL_HEADER_SIZE as u64 * 2 + len as u64,
                ))?;
                let n = f.f.read(&mut buffer[end_offset..])?;
                if n == 0 {
                    let next_f = if use_wal_1 { &f2 } else { &f1 };
                    if next_f.relative_lsn < current_lsn.get() {
                        break;
                    }
                    use_wal_1 = !use_wal_1;
                    start_offset = 0;
                    end_offset = 0;
                    continue;
                }

                end_offset += n;
                entry = WalEntry::decode(&buffer[start_offset..end_offset]);
            };

            match entry {
                WalDecodeResult::Ok(entry) => {
                    let lsn = current_lsn;
                    let entry_size = entry.size();
                    start_offset += entry_size;
                    current_lsn.add_assign(entry_size as u64);
                    next_lsn = current_lsn;
                    handler(lsn, entry)?;
                }
                WalDecodeResult::NeedMoreBytes | WalDecodeResult::Incomplete => {
                    let next_f = if use_wal_1 { &f2 } else { &f1 };
                    if next_f.relative_lsn < current_lsn.get() {
                        break;
                    }
                    use_wal_1 = !use_wal_1;
                    start_offset = 0;
                    end_offset = 0;
                }
                WalDecodeResult::Err(err) => return Err(err),
            }
        }
    }

    Ok(Wal::new(f1.into(), f2.into(), use_wal_1, next_lsn))
}

fn recover_wal_file(mut f: File) -> anyhow::Result<RecoveringWalFile> {
    let file_size = f.metadata()?.len();
    if file_size < WAL_HEADER_SIZE as u64 * 2 {
        return Ok(RecoveringWalFile {
            f,
            relative_lsn: 0,
            checkpoint: None,
            is_empty: true,
        });
    }

    let mut buff = [0u8; WAL_HEADER_SIZE * 2];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut buff)?;

    let Some(header) = WalHeader::decode(&buff[..WAL_HEADER_SIZE])
        .or_else(|| WalHeader::decode(&buff[WAL_HEADER_SIZE..]))
    else {
        return Err(anyhow!(
            "corrupted wal file header, both header segment are corrupted"
        ));
    };

    log::debug!("wal_header_decoded header={header:?}");

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

struct RecoveringWalFile {
    f: File,
    relative_lsn: u64,
    checkpoint: Option<Lsn>,
    is_empty: bool,
}

impl From<RecoveringWalFile> for WalFile {
    fn from(value: RecoveringWalFile) -> Self {
        Self {
            f: value.f,
            relative_lsn: value.relative_lsn,
            is_empty: value.is_empty,
            checkpoint: value.checkpoint.is_some(),
        }
    }
}

impl std::fmt::Debug for RecoveringWalFile {
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
    use rand::Rng;

    #[test]
    fn test_flushing() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        let mut last_seen_lsn = None;
        for i in 1..=10 {
            let lsn = wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
            if let Some(last_seen_lsn) = last_seen_lsn {
                assert!(
                    lsn > last_seen_lsn,
                    "last_seen_lsn={last_seen_lsn:?} lsn={lsn:?}"
                );
            }
            last_seen_lsn = Some(lsn);
        }
        drop(wal);

        let wal = recover(dir.path(), |_, _| {
            panic!("since the wal is not flushed yet, there should be no entry")
        })?;

        let mut last_seen_lsn = None;
        for i in 1..=10 {
            let lsn = wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
            if let Some(last_seen_lsn) = last_seen_lsn {
                assert!(
                    lsn > last_seen_lsn,
                    "last_seen_lsn={last_seen_lsn:?} lsn={lsn:?}"
                );
            }
            last_seen_lsn = Some(lsn);
        }
        wal.trigger_flush()?;
        drop(wal);

        let mut i = 1;
        let mut last_seen_lsn = None;
        let wal = recover(dir.path(), |lsn, entry| {
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
            Ok(())
        })?;
        assert_eq!(11, i);
        drop(wal);

        Ok(())
    }

    #[test]
    fn test_checkpoint() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        for i in 1..=10 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        let checkpoint_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: TxState::None,
                root: PageId::new(123),
                freelist: PageId::new(321),
                page_count: 99,
            },
        })?;
        for i in 11..=15 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        for i in 16..=20 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        drop(wal);

        let mut i = 11;
        let mut checkpoint_consumed = false;
        let wal = recover(dir.path(), |lsn, entry| {
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

            Ok(())
        })?;
        assert_eq!(16, i);
        drop(wal);

        Ok(())
    }

    #[test]
    fn test_recovering_from_wal_1_and_2() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        for i in 1..=10 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        let checkpoint_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: TxState::None,
                root: PageId::new(123),
                freelist: PageId::new(321),
                page_count: 99,
            },
        })?;
        for i in 11..=15 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.trigger_flush()?;
        for i in 16..=20 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        for i in 21..=25 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.trigger_flush()?;
        for i in 26..=30 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        drop(wal);

        let mut i = 11;
        let mut checkpoint_consumed = false;
        let wal = recover(dir.path(), |lsn, entry| {
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

            Ok(())
        })?;
        assert_eq!(26, i);

        for i in 26..=30 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        let checkpoint_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Checkpoint {
                active_tx: TxState::None,
                root: None,
                freelist: None,
                page_count: 99,
            },
        })?;
        for i in 31..=35 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        for i in 36..=40 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        wal.trigger_flush()?;
        for i in 41..=45 {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::LeafInit {
                    txid: TxId::new(i).unwrap(),
                    pgid: PageId::new(1000 + i).unwrap(),
                },
            })?;
        }
        drop(wal);

        let mut i = 31;
        let mut checkpoint_consumed = false;
        let wal = recover(dir.path(), |lsn, entry| {
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

            Ok(())
        })?;
        assert_eq!(41, i);
        drop(wal);

        Ok(())
    }

    #[test]
    fn test_wal_1_and_2_have_checkpoint() -> anyhow::Result<()> {
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

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        for i in 1..=5 {
            wal.append_log(dummy_entry(i))?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry())?;
        for i in 6..=10 {
            wal.append_log(dummy_entry(i))?;
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        for i in 11..=15 {
            wal.append_log(dummy_entry(i))?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry())?;
        for i in 16..=20 {
            wal.append_log(dummy_entry(i))?;
        }
        wal.trigger_flush()?;
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        drop(wal);

        let mut i = 16;
        let mut checkpoint_consumed = false;
        let wal = recover(dir.path(), |lsn, entry| {
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

            Ok(())
        })?;
        assert_eq!(21, i);

        for i in 21..=25 {
            wal.append_log(dummy_entry(i))?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry())?;
        for i in 26..=30 {
            wal.append_log(dummy_entry(i))?;
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;
        for i in 31..=35 {
            wal.append_log(dummy_entry(i))?;
        }
        let checkpoint_lsn = wal.append_log(checkpoint_entry())?;
        for i in 36..=40 {
            wal.append_log(dummy_entry(i))?;
        }
        wal.trigger_flush()?;
        wal.complete_checkpoint(checkpoint_lsn)?;
        wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::End {
                txid: TxId::new(1).unwrap(),
            },
        })?;

        drop(wal);

        let mut i = 36;
        let mut checkpoint_consumed = false;
        let wal = recover(dir.path(), |lsn, entry| {
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

            Ok(())
        })?;
        assert_eq!(41, i);
        drop(wal);

        Ok(())
    }

    #[test]
    fn test_million_logs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let mut r = rand::thread_rng();
        let mut i = 1;
        let mut last_lsn = Lsn::new(0);
        let mut last_checkpoint = None;

        for _ in 0..100 {
            let mut checkpoint_consumed = false;
            let wal = recover(dir.path(), |lsn, entry| {
                assert!(lsn >= last_lsn);
                last_lsn = lsn;
                assert!(entry.clr.is_none());

                if matches!(entry.kind, WalKind::End { .. }) {
                    return Ok(());
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

                Ok(())
            })?;

            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })?;
                i += 1;
            }
            wal.trigger_flush()?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })?;
                i += 1;
            }
            let checkpoint_lsn = wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: TxState::None,
                    root: None,
                    freelist: None,
                    page_count: 99,
                },
            })?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })?;
                i += 1;
            }
            wal.complete_checkpoint(checkpoint_lsn)?;
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::End {
                    txid: TxId::new(1).unwrap(),
                },
            })?;
            last_checkpoint = Some(checkpoint_lsn);
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })?;
                i += 1;
            }
            wal.trigger_flush()?;
            for _ in 0..r.gen_range(1..=8000) {
                wal.append_log(WalEntry {
                    clr: None,
                    kind: WalKind::LeafInit {
                        txid: TxId::new(i).unwrap(),
                        pgid: PageId::new(1000 + i).unwrap(),
                    },
                })?;
                i += 1;
            }
            drop(wal);
        }

        Ok(())
    }

    #[test]
    fn test_iterate_backward() -> anyhow::Result<()> {
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

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        for i in 1..=5 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone())?;
            entries.push((lsn, entry));
        }
        let entry = checkpoint_entry();
        let checkpoint_lsn = wal.append_log(entry.clone())?;
        entries.push((checkpoint_lsn, entry));
        for i in 6..=10 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone())?;
            entries.push((lsn, entry));
        }
        wal.complete_checkpoint(checkpoint_lsn)?;
        let rollback_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Rollback {
                txid: TxId::new(1).unwrap(),
            },
        })?;

        wal.iter_back(rollback_lsn, |lsn, entry| {
            let Some((expected_lsn, expected_entry)) = entries.pop() else {
                return Ok(true);
            };
            assert_eq!(expected_lsn, lsn);
            assert_eq!(expected_entry, entry);

            wal.append_log(WalEntry {
                clr: Some(lsn),
                kind: WalKind::LeafResetForUndo {
                    txid: TxId::new(1).unwrap(),
                    pgid: PageId::new(1000).unwrap(),
                },
            })?;

            Ok(false)
        })?;
        assert!(entries.is_empty());

        Ok(())
    }

    #[test]
    fn test_iterate_backward_from_buffer() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let dummy_entry = |i: u64| WalEntry {
            clr: None,
            kind: WalKind::LeafInit {
                txid: TxId::new(i).unwrap(),
                pgid: PageId::new(1000 + i).unwrap(),
            },
        };

        let mut entries = vec![];

        let wal = recover(dir.path(), |_, _| Ok(()))?;
        for i in 1..=5 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone())?;
            entries.push((lsn, entry));
        }
        wal.trigger_flush()?;
        for i in 6..=10 {
            let entry = dummy_entry(i);
            let lsn = wal.append_log(entry.clone())?;
            entries.push((lsn, entry));
        }
        let rollback_lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::Rollback {
                txid: TxId::new(1).unwrap(),
            },
        })?;

        wal.iter_back(rollback_lsn, |lsn, entry| {
            println!("item> {lsn:?} {entry:?}");
            let Some((expected_lsn, expected_entry)) = entries.pop() else {
                return Ok(true);
            };
            assert_eq!(expected_lsn, lsn);
            assert_eq!(expected_entry, entry);

            wal.append_log(WalEntry {
                clr: Some(lsn),
                kind: WalKind::LeafResetForUndo {
                    txid: TxId::new(1).unwrap(),
                    pgid: PageId::new(1000).unwrap(),
                },
            })?;

            Ok(false)
        })?;
        assert!(entries.is_empty());

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
