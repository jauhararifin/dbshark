use crate::pager::{PageId, PageIdExt};
use parking_lot::Mutex;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TxId(NonZeroU64);

impl TxId {
    pub(crate) fn new(id: u64) -> Option<Self> {
        if id >= 1 << 56 {
            None
        } else {
            NonZeroU64::new(id).map(Self)
        }
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Lsn(NonZeroU64);

impl Lsn {
    pub(crate) fn new(lsn: u64) -> Option<Self> {
        NonZeroU64::new(lsn).map(Self)
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.get()
    }

    pub(crate) fn add(&self, offset: usize) -> Self {
        let v = self.0.get() + offset as u64;
        Self::new(v).expect("should not overflow")
    }

    pub(crate) fn from_be_bytes(lsn: [u8; 8]) -> Option<Self> {
        Self::new(u64::from_be_bytes(lsn))
    }
}

pub(crate) trait LsnExt {
    fn to_be_bytes(&self) -> [u8; 8];
}

impl LsnExt for Lsn {
    fn to_be_bytes(&self) -> [u8; 8] {
        self.0.get().to_be_bytes()
    }
}
impl LsnExt for Option<Lsn> {
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(pgid) = self {
            pgid.to_be_bytes()
        } else {
            0u64.to_be_bytes()
        }
    }
}

// TODO: when a txn is committed, we need to flush their commit log record to disk. But,
// we can actually start the next transaction right away without waiting for the flush to complete.
// We just need to block the `commit()` call until the flush is done. This way, we can improve
// the throughput of the system. Also flushing the log to disk can be done in a separate thread
// making the flushing process non-blocking.
pub(crate) struct Wal {
    f: Mutex<File>,

    internal: Mutex<WalInternal>,
}

struct WalInternal {
    buffer: Vec<u8>,
    offset_end: usize,
    next_lsn: Lsn,
}

impl Wal {
    pub(crate) fn new(f: File, page_size: usize) -> Self {
        let buff_size = page_size * 100;
        Wal {
            f: Mutex::new(f),

            internal: Mutex::new(WalInternal {
                buffer: vec![0u8; buff_size],
                offset_end: 0,
                // TODO: this should be read from the file
                // the WAL file header should encodes these information:
                // - last flushed lsn
                // - lsn of last checkpoint
                next_lsn: Lsn::new(8).unwrap(),
            }),
        }
    }

    pub(crate) fn append(&self, txid: TxId, record: WalRecord) -> anyhow::Result<Lsn> {
        let entry = WalByteEntry { txid, record };
        let size = entry.size();

        let mut internal = self.internal.lock();
        if internal.offset_end + size > internal.buffer.len() {
            Self::flush(&mut *self.f.lock(), &mut *internal)?;
        }

        let offset_end = internal.offset_end;
        let lsn = internal.next_lsn;
        entry.encode(&mut internal.buffer[offset_end..offset_end + size]);
        internal.offset_end += size;
        internal.next_lsn = internal.next_lsn.add(size);

        Ok(lsn)
    }

    pub(crate) fn sync(&self, lsn: Lsn) -> anyhow::Result<()> {
        let mut internal = self.internal.lock();
        Self::flush(&mut *self.f.lock(), &mut *internal)
    }

    fn flush(f: &mut File, internal: &mut WalInternal) -> anyhow::Result<()> {
        f.write_all(&internal.buffer[..internal.offset_end])?;
        f.sync_all()?;
        internal.offset_end = 0;
        Ok(())
    }

    pub(crate) fn shutdown(self) -> anyhow::Result<()> {
        let internal = self.internal.into_inner();

        let mut f = self.f.into_inner();
        f.write_all(&internal.buffer[..internal.offset_end])?;
        f.sync_all()?;

        Ok(())
    }
}

pub(crate) enum WalRecord<'a> {
    Begin,
    Commit,
    Rollback,
    End,

    InteriorInit {
        pgid: PageId,
        last: PageId,
    },
    InteriorInsert {
        pgid: PageId,
        index: usize,
        raw: &'a [u8],
        ptr: PageId,
        overflow: Option<PageId>,
        key_size: usize,
    },
    InteriorDelete {
        pgid: PageId,
        index: usize,
        old_raw: &'a [u8],
        old_ptr: PageId,
        old_overflow: Option<PageId>,
        old_key_size: usize,
    },
}

impl<'a> WalRecord<'a> {
    fn kind(&self) -> u8 {
        match self {
            WalRecord::Begin => 1,
            WalRecord::Commit => 2,
            WalRecord::Rollback => 3,
            WalRecord::End => 4,

            WalRecord::InteriorInit { .. } => 10,
            WalRecord::InteriorInsert { .. } => 11,
            WalRecord::InteriorDelete { .. } => 12,
        }
    }

    fn size(&self) -> usize {
        match self {
            WalRecord::Begin | WalRecord::Commit | WalRecord::Rollback | WalRecord::End => 0,
            WalRecord::InteriorInit { .. } => 16,
            WalRecord::InteriorInsert { .. } => 32,
            WalRecord::InteriorDelete { .. } => 32,
        }
    }

    fn encode(&self, buff: &mut [u8]) {
        match self {
            WalRecord::Begin | WalRecord::Commit | WalRecord::Rollback | WalRecord::End => (),
            WalRecord::InteriorInit { pgid, last } => {
                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..16].copy_from_slice(&last.get().to_be_bytes());
            }
            WalRecord::InteriorInsert {
                pgid,
                index,
                raw,
                ptr,
                overflow,
                key_size,
            } => {
                assert!(raw.len() <= u16::MAX as usize);
                assert!(*key_size <= u32::MAX as usize);
                assert!(*index <= u16::MAX as usize);

                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..12].copy_from_slice(&(*key_size as u32).to_be_bytes());
                buff[12..14].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[14..16].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[16..24].copy_from_slice(&ptr.to_be_bytes());
                buff[24..32].copy_from_slice(&overflow.to_be_bytes());
            }
            WalRecord::InteriorDelete {
                pgid,
                index,
                old_raw,
                old_ptr,
                old_overflow,
                old_key_size,
            } => {
                assert!(old_raw.len() <= u16::MAX as usize);
                assert!(*old_key_size <= u32::MAX as usize);
                assert!(*index <= u16::MAX as usize);

                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..12].copy_from_slice(&(*old_key_size as u32).to_be_bytes());
                buff[12..14].copy_from_slice(&(old_raw.len() as u16).to_be_bytes());
                buff[14..16].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[16..24].copy_from_slice(&old_ptr.to_be_bytes());
                buff[24..32].copy_from_slice(&old_overflow.to_be_bytes());
            }
        }
    }
}

struct WalByteEntry<'a> {
    txid: TxId,
    record: WalRecord<'a>,
}

impl<'a> WalByteEntry<'a> {
    fn new(txid: TxId, record: WalRecord<'a>) -> Self {
        Self { txid, record }
    }

    fn size(&self) -> usize {
        // (8 bytes for txid+kind) + (2 byte for entry size) + (2 byte for entry size) (entry) + (padding) (8 checksum)
        8 + pad8(2 + 2 + self.record.size()) + 8
    }

    fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(&self.txid.get().to_be_bytes());
        buff[0] = self.record.kind();

        let size = self.size();
        assert!(size < 1 << 16);
        let record_size = self.record.size();
        assert!(record_size < 1 << 16);

        buff[8..10].copy_from_slice(&(size as u16).to_be_bytes());
        buff[10..12].copy_from_slice(&(record_size as u16).to_be_bytes());
        self.record.encode(&mut buff[10..10 + record_size]);

        let next = pad8(10 + record_size);
        buff[10 + record_size..next].fill(0);

        // TODO: we may want to add the entry size as the footer to help
        // us traverse the log file in reverse order. This can be useful
        // for rollback purposes.

        let checksum = crc64::crc64(0, &buff[0..next]);
        buff[next..next + 8].copy_from_slice(&checksum.to_be_bytes());
    }
}

fn pad8(size: usize) -> usize {
    (size + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txid() {
        assert_eq!(None, TxId::new(0), "txid cannot be zero");
        assert_eq!(1, TxId::new(1).unwrap().get());
        assert_eq!(10, TxId::new(10).unwrap().get());
        assert_eq!(10, TxId::new(10).unwrap().get());
        let max_txid = TxId::new(72057594037927935).unwrap();
        assert_eq!(72057594037927935, max_txid.get());
        assert_eq!(
            0,
            max_txid.get().to_be_bytes()[0],
            "first MSB of txid should be zero"
        );

        assert_eq!(
            None,
            TxId::new(72057594037927936),
            "txid cannot be 2^56 or graeter"
        );
    }

    #[test]
    fn test_encode() {
        let entry = WalByteEntry {
            txid: TxId::new(1).unwrap(),
            record: WalRecord::Begin,
        };
        let mut buff = vec![0u8; entry.size()];
        entry.encode(&mut buff);
        assert_eq!(
            &[
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // kind=1, txid=1
                0x00, 0x18, // record size
                0x00, 0x00, // entry size
                // the entry is zero bytes
                0x00, 0x00, 0x00, 0x00, // pad to 8 bytes
                0x7f, 0x4f, 0x12, 0xf4, 0xb5, 0x4b, 0x09, 0xff,
            ],
            buff.as_slice()
        );
    }

    #[test]
    fn test_wal() {
        let dir = tempfile::tempdir().unwrap();

        let file_path = dir.path().join("test.wal");
        let mut file = File::create(file_path).unwrap();

        let page_size = 256;
        let wal = Wal::new(file, page_size);

        let lsn = wal
            .append(TxId::new(1).unwrap(), WalRecord::Begin {})
            .unwrap();
        assert_eq!(8, lsn.get());

        let lsn = wal
            .append(
                TxId::new(1).unwrap(),
                WalRecord::InteriorInit {
                    pgid: PageId::new(10).unwrap(),
                    last: PageId::new(10).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(32, lsn.get());

        dir.close().unwrap();
    }
}
