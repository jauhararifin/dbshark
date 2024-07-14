use crate::pager::Pager;
use crate::wal::{TxId, TxIdExt, Wal};
use anyhow::anyhow;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::{fs::File, path::Path};

pub struct Db {
    pager: Pager,
}

impl Db {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let wal_path = path.with_extension("wal");

        let mut db_file = File::create(path)?;
        let header = Self::load_db_header(&mut db_file)?;
        let page_size = header.page_size as usize;

        let wal_file = File::create(wal_path)?;
        let wal = Wal::new(wal_file, page_size);

        let pager = Pager::new(db_file, wal, page_size, 1000)?;

        Ok(Self { pager })
    }

    fn load_db_header(f: &mut File) -> anyhow::Result<Header> {
        let meta = f.metadata()?;
        let size = meta.size();

        if size == 0 {
            return Self::init_db(f);
        }

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        f.read_exact(&mut buff)?;

        if buff[..8].cmp(b"dbdbdbdb").is_ne() {
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
        };

        let mut buff = vec![0; 2 * DB_HEADER_SIZE];
        header.encode(&mut buff[..DB_HEADER_SIZE]);
        header.encode(&mut buff[DB_HEADER_SIZE..DB_HEADER_SIZE * 2]);
        f.write_all(&buff)?;

        Ok(header)
    }
}

const DB_HEADER_SIZE: usize = 32;
const DEFAULT_PAGE_SIZE: usize = 0x1000;

struct Header {
    version: u32,
    page_size: u32,
    last_txid: Option<TxId>,
}

impl Header {
    fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(b"dbdbdbdb");
        buff[8..12].copy_from_slice(&self.version.to_be_bytes());
        buff[12..16].copy_from_slice(&self.page_size.to_be_bytes());
        buff[16..24].copy_from_slice(&self.last_txid.to_be_bytes());
        let checksum = crc64::crc64(0, &buff[0..24]);
        buff[24..32].copy_from_slice(&checksum.to_be_bytes());
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

        Some(Self {
            version,
            page_size,
            last_txid,
        })
    }
}
