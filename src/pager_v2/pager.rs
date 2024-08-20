use crate::id::PageId;
use crate::pager_v2::page::PageMeta;
use anyhow::anyhow;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct Pager {
    f: Mutex<File>,
    dbuff: Mutex<File>,
}

const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;

impl Pager {
    pub(crate) fn new(path: &Path, page_size: usize, n: usize) -> anyhow::Result<Self> {
        let db_path = path.join("main");
        let double_buff_path = path.join("dbuff");

        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(db_path)?;
        if !db_file.metadata()?.is_file() {
            return Err(anyhow!("db file is not a regular file"));
        }
        let mut double_buff_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(double_buff_path)?;
        if !double_buff_file.metadata()?.is_file() {
            return Err(anyhow!("double buffer file is not a regular file"));
        }

        Self::check_page_size(page_size)?;
        if n < 10 {
            return Err(anyhow!(
                "the size of buffer poll must be at least 10, but got {n}",
            ));
        }

        Self::recover_non_atomic_writes(&mut db_file, &mut double_buff_file, page_size)?;

        Ok(Self {
            f: Mutex::new(db_file),
            dbuff: Mutex::new(double_buff_file),
        })
    }

    fn check_page_size(page_size: usize) -> anyhow::Result<()> {
        if page_size.count_ones() != 1 {
            return Err(anyhow!(
                "page size must be a power of 2, but got {}",
                page_size
            ));
        }

        if page_size < MINIMUM_PAGE_SIZE {
            return Err(anyhow!(
                "page size must be at least {} bytes, but got {}",
                MINIMUM_PAGE_SIZE,
                page_size
            ));
        }
        if page_size > MAXIMUM_PAGE_SIZE {
            return Err(anyhow!(
                "page size must be at most 16KB, but got {}",
                page_size
            ));
        }

        Ok(())
    }

    fn recover_non_atomic_writes(
        f: &mut File,
        dbuff: &mut File,
        page_size: usize,
    ) -> anyhow::Result<()> {
        let size = dbuff.metadata()?.len();
        let count = (size as usize) / page_size;

        let mut buff = vec![0u8; page_size * count];
        dbuff.seek(SeekFrom::Start(0))?;
        dbuff.read_exact(&mut buff)?;

        for i in 0..count {
            let buff = &buff[i * page_size..(i + 1) * page_size];
            let Some(meta) = Self::decode_page(buff)? else {
                continue;
            };
            Self::write_page_no_sync(f, meta.id(), buff)?;
        }

        f.sync_all()?;
        Ok(())
    }

    fn decode_page(buff: &[u8]) -> anyhow::Result<Option<PageMeta>> {
        todo!();
    }

    fn write_page_no_sync(f: &mut File, id: PageId, buff: &[u8]) -> anyhow::Result<()> {
        let page_size = buff.len() as u64;
        let file_size = f.metadata()?.len();
        let min_size = id.get() * page_size + page_size;
        if min_size > file_size {
            f.set_len(id.get() * page_size + page_size)?;
        }
        f.seek(SeekFrom::Start(id.get() * page_size))?;
        f.write_all(buff)?;
        Ok(())
    }
}
