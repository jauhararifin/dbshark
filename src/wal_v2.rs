use crate::id::Lsn;
use crate::log::{WalDecodeResult, WalEntry, WalHeader, WAL_HEADER_SIZE};
use crate::pager::MAXIMUM_PAGE_SIZE;
use anyhow::{anyhow, Context};
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread::spawn;

pub(crate) struct Wal {
    f1: Arc<Mutex<WalFile>>,
    f2: Arc<Mutex<WalFile>>,
    buffer: Arc<RwLock<Buffer>>,
    internal: Arc<RwLock<WalInternal>>,
    flush_trigger: SyncSender<()>,
}

struct WalFile {
    f: File,
    relative_lsn: u64,
    next: Lsn,
    is_empty: bool,
}

struct Buffer {
    buff: Vec<u8>,
    offset_start: usize,
    offset_end: usize,
}

impl Buffer {
    #[inline]
    fn len(&self) -> usize {
        if self.offset_start < self.offset_end {
            self.offset_end - self.offset_start
        } else {
            self.offset_end + self.buff.len() - self.offset_start
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
    pub(crate) fn new(path: &Path, page_size: usize) -> anyhow::Result<Self> {
        let wal_path_1 = path.join("wal_1");
        let wal_file_1 = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path_1)?;
        if !wal_file_1.metadata()?.is_file() {
            return Err(anyhow!("{wal_path_1:?} is not a regular file"));
        }
        let f1 = Self::init_wal_file(wal_file_1).context("cannot init wal file {wal_path_1:?}")?;

        let wal_path_2 = path.join("wal_2");
        let wal_file_2 = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path_2)?;
        if !wal_file_2.metadata()?.is_file() {
            return Err(anyhow!("{wal_path_2:?} is not a regular file"));
        }
        let f2 = Self::init_wal_file(wal_file_2).context("cannot init wal file {wal_path_2:?}")?;

        let use_wal_1 = f1.next >= f2.next;
        let next = if use_wal_1 { f1.next } else { f2.next };
        let internal = Arc::new(RwLock::new(WalInternal {
            temp_buffer: vec![0u8; page_size],
            use_wal_1,
            next,
            first_unflushed: next,
        }));

        let f1 = Arc::new(Mutex::new(f1));
        let f2 = Arc::new(Mutex::new(f2));
        let buffer = Arc::new(RwLock::new(Buffer {
            buff: vec![0u8; page_size * 20],
            offset_start: 0,
            offset_end: 0,
        }));

        let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(0);
        {
            let internal = internal.clone();
            let buffer = buffer.clone();
            let f1 = f1.clone();
            let f2 = f2.clone();
            spawn(move || loop {
                loop {
                    let result = receiver.recv_timeout(std::time::Duration::from_secs(3600));
                    if result == Err(RecvTimeoutError::Disconnected) {
                        break;
                    }
                    let mut internal = internal.write();
                    if let Err(err) = Self::flush(&mut internal, &mut buffer.write(), &f1, &f2) {
                        log::error!("wal_flush_error err={err}");
                    }
                }
            });
        }

        Ok(Self {
            f1,
            f2,
            buffer,
            internal,
            flush_trigger: sender,
        })
    }

    fn init_wal_file(mut f: File) -> anyhow::Result<WalFile> {
        let file_size = f.metadata()?.len();
        if file_size < WAL_HEADER_SIZE as u64 * 2 {
            let lsn = Lsn::new(WAL_HEADER_SIZE as u64 * 2).unwrap();
            return Ok(WalFile {
                f,
                relative_lsn: 0,
                next: lsn,
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

        if header.version != 0 {
            return Err(anyhow!("only wal version 0 is supported"));
        }

        let mut next = Lsn::new(WAL_HEADER_SIZE as u64 * 2).unwrap();
        let first_lsn = Lsn::new(header.relative_lsn + WAL_HEADER_SIZE as u64 * 2).unwrap();
        let mut iter = WalIter::new(&mut f, first_lsn)?;
        while let Some((lsn, _)) = iter.next()? {
            next = lsn;
        }

        Ok(WalFile {
            f,
            relative_lsn: header.relative_lsn,
            next,
            is_empty: false,
        })
    }

    pub(crate) fn complete_checkpoint(&self, checkpoint_lsn: Lsn) -> anyhow::Result<()> {
        let mut internal = self.internal.write();
        let internal = &mut *internal;

        let f = if internal.use_wal_1 {
            &self.f1
        } else {
            &self.f2
        };
        let mut f = f.lock();

        let header = WalHeader {
            version: 0,
            checkpoint: Some(checkpoint_lsn),
            relative_lsn: f.relative_lsn,
        };

        let mut buff = [0u8; WAL_HEADER_SIZE * 2];
        header.encode(&mut buff[..WAL_HEADER_SIZE]);
        header.encode(&mut buff[WAL_HEADER_SIZE..]);
        f.f.seek(SeekFrom::Start(0))?;
        f.f.write_all(&buff)?;
        f.is_empty = false;

        internal.use_wal_1 = !internal.use_wal_1;

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

        let offset_end = buffer.offset_end;
        if offset_end + size > buffer.size() {
            let part1 = buffer.size() - offset_end;
            let part2 = size - part1;
            entry.encode(&mut internal.temp_buffer[..size]);
            buffer.buff[offset_end..].copy_from_slice(&internal.temp_buffer[..part1]);
            buffer.buff[..part2].copy_from_slice(&internal.temp_buffer[part1..]);
            buffer.offset_end = part2;
        } else {
            entry.encode(&mut buffer.buff[offset_end..offset_end + size]);
            buffer.offset_end += size;
        }
        let lsn = internal.next;
        internal.next.add_assign(size as u64);

        if buffer.len() > buffer.size() / 4 {
            let _ = self.flush_trigger.try_send(());
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
        if f.is_empty {
            let mut buff = [0u8; WAL_HEADER_SIZE * 2];
            let header = WalHeader {
                version: 0,
                checkpoint: None,
                relative_lsn: f.relative_lsn,
            };
            header.encode(&mut buff[..WAL_HEADER_SIZE]);
            header.encode(&mut buff[WAL_HEADER_SIZE..]);
            f.f.seek(SeekFrom::Start(0))?;
            f.f.write_all(&buff)?;
            f.is_empty = false;
        }

        let offset = f.next.get() - f.relative_lsn;
        f.f.seek(SeekFrom::Start(offset))?;

        if buffer.offset_end < buffer.offset_start {
            f.f.write_all(&buffer.buff[buffer.offset_start..])?;
            f.f.write_all(&buffer.buff[..buffer.offset_end])?;
        } else {
            f.f.write_all(&buffer.buff[buffer.offset_start..buffer.offset_end])?;
        }
        f.f.sync_all()?;

        buffer.offset_start = buffer.offset_end;
        internal.first_unflushed = internal.next;

        Ok(())
    }

    pub(crate) fn sync(&self, lsn: Lsn) -> anyhow::Result<()> {
        let internal = self.internal.read();
        if internal.first_unflushed > lsn {
            return Ok(());
        }
        drop(internal);

        let mut internal = self.internal.write();
        if internal.first_unflushed > lsn {
            return Ok(());
        }

        let mut buffer = self.buffer.write();
        Self::flush(&mut internal, &mut buffer, &self.f1, &self.f2)?;
        Ok(())
    }
}

pub(crate) struct WalIter<'a> {
    f: &'a mut File,
    lsn: Lsn,
    buffer: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
}

impl<'a> WalIter<'a> {
    fn new(f: &'a mut File, relative_lsn: Lsn) -> anyhow::Result<Self> {
        f.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64 * 2))?;
        Ok(Self {
            f,
            lsn: relative_lsn.add(WAL_HEADER_SIZE as u64 * 2),
            buffer: vec![0u8; MAXIMUM_PAGE_SIZE * 4],
            start_offset: 0,
            end_offset: 0,
        })
    }

    pub(crate) fn next(&mut self) -> anyhow::Result<Option<(Lsn, WalEntry)>> {
        let buff = &self.buffer[self.start_offset..self.end_offset];
        // TODO: this is a hack, come back and fix this.
        let buff = unsafe { std::mem::transmute::<&[u8], &[u8]>(buff) };
        let mut entry = WalEntry::decode(buff);

        if let WalDecodeResult::NeedMoreBytes = entry {
            let len = self.end_offset - self.start_offset;
            for i in 0..len {
                self.buffer[i] = self.buffer[self.start_offset + i];
            }
            self.start_offset = 0;
            self.end_offset = len;

            let n = self.f.read(&mut self.buffer[self.end_offset..])?;
            if n == 0 {
                return Ok(None);
            }

            self.end_offset += n;
            entry = WalEntry::decode(&self.buffer[self.start_offset..self.end_offset])
        };

        match entry {
            WalDecodeResult::Ok(entry) => {
                let lsn = self.lsn;
                let entry_size = entry.size();
                self.start_offset += entry_size;
                self.lsn.add_assign(entry_size as u64);
                return Ok(Some((lsn, entry)));
            }
            WalDecodeResult::NeedMoreBytes => {
                unreachable!();
            }
            WalDecodeResult::Incomplete => {
                return Ok(None);
            }
            WalDecodeResult::Err(err) => {
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{PageId, TxId};
    use crate::log::WalKind;

    #[test]
    fn test_init_empty_wal() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;

        let wal = Wal::new(dir.path(), 4096)?;
        let lsn = wal.append_log(WalEntry {
            clr: None,
            kind: WalKind::LeafInit {
                txid: TxId::new(1).unwrap(),
                pgid: PageId::new(1).unwrap(),
            },
        })?;
        wal.sync(lsn)?;

        Ok(())
    }
}
