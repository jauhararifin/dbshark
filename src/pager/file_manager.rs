use crate::file_lock::FileLock;
use crate::id::{Lsn, PageId};
use crate::pager::log::WalSync;
use crate::pager::page::PageMeta;
use anyhow::anyhow;
use indexmap::IndexSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct FileManager {
    main: File,
    double_buff: File,
    page_size: usize,

    n: usize,
    pages: Box<[u8]>,
    pgids: IndexSet<PageId>,
    lsns: Box<[Lsn]>,
}

impl FileManager {
    pub(crate) fn new(path: &Path, page_size: usize, n: usize) -> anyhow::Result<Self> {
        let main_path = path.join("main");
        let double_buff_path = path.join("dbuff");

        let mut main = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(main_path)?
            .lock()?;
        if !main.metadata()?.is_file() {
            return Err(anyhow!("db file is not a regular file"));
        }
        let mut double_buff = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(double_buff_path)?
            .lock()?;
        if !double_buff.metadata()?.is_file() {
            return Err(anyhow!("double buffer file is not a regular file"));
        }

        Self::recover_non_atomic_writes(&mut main, &mut double_buff, page_size)?;
        Ok(Self {
            main,
            double_buff,
            page_size,

            n,
            pages: vec![0u8; page_size * n].into_boxed_slice(),
            pgids: IndexSet::with_capacity(n),
            lsns: vec![Lsn::new(0); n].into_boxed_slice(),
        })
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
            let Some(meta) = PageMeta::decode(buff)? else {
                continue;
            };
            Self::write_page_no_sync(f, page_size as u64, meta.id(), buff)?;
        }

        f.sync_all()?;
        Ok(())
    }

    fn write_page_no_sync(
        f: &mut File,
        page_size: u64,
        id: PageId,
        buff: &[u8],
    ) -> anyhow::Result<()> {
        let file_size = f.metadata()?.len();
        let min_size = id.get() * page_size + page_size;
        if min_size > file_size {
            f.set_len(id.get() * page_size + page_size)?;
        }
        f.seek(SeekFrom::Start(id.get() * page_size))?;
        f.write_all(buff)?;
        Ok(())
    }

    pub(crate) fn spill(
        &mut self,
        wal: &impl WalSync,
        meta: &PageMeta,
        buff: &mut [u8],
    ) -> anyhow::Result<()> {
        assert_eq!(self.page_size, buff.len());
        meta.encode(buff)?;

        let index = if let Some((i, _)) = self.pgids.get_full(&meta.id) {
            i
        } else {
            let is_full = self.pgids.len() >= self.n;
            if is_full {
                self.sync(wal)?;
            }
            let i = self.pgids.len();
            self.pgids.insert(meta.id);
            i
        };
        self.pages[index * self.page_size..(index + 1) * self.page_size].copy_from_slice(buff);
        self.lsns[index] = meta.lsn;

        Ok(())
    }

    pub(crate) fn sync(&mut self, wal: &impl WalSync) -> anyhow::Result<()> {
        self.double_buff.set_len(0)?;
        self.double_buff.seek(SeekFrom::Start(0))?;
        self.double_buff.write_all(&self.pages)?;
        self.double_buff.sync_all()?;

        if let Some(max_lsn) = (0..self.pgids.len()).map(|i| self.lsns[i]).max() {
            wal.sync(max_lsn)?;
        }

        for (i, pgid) in self.pgids.iter().enumerate() {
            // TODO: maybe we can use vectorized write to write them all in one single syscall
            let page_size = self.page_size as u64;
            let file_size = self.main.metadata()?.len();
            let min_size = pgid.get() * page_size + page_size;
            if min_size > file_size {
                self.main.set_len(pgid.get() * page_size + page_size)?;
            }
            self.main.seek(SeekFrom::Start(pgid.get() * page_size))?;
            let buff = &self.pages[i * self.page_size..(i + 1) * self.page_size];
            self.main.write_all(buff)?;
        }
        self.main.sync_all()?;
        self.pgids.clear();

        Ok(())
    }

    pub(crate) fn get(&mut self, pgid: PageId, buff: &mut [u8]) -> anyhow::Result<bool> {
        if let Some((i, _)) = self.pgids.get_full(&pgid) {
            buff.copy_from_slice(&self.pages[i * self.page_size..(i + 1) * self.page_size]);
            Ok(true)
        } else {
            let page_size = self.page_size as u64;
            let file_size = self.main.metadata()?.len();
            let min_size = pgid.get() * page_size + page_size;
            if min_size > file_size {
                return Ok(false);
            }
            self.main.seek(SeekFrom::Start(pgid.get() * page_size))?;
            self.main.read_exact(buff)?;
            Ok(true)
        }
    }
}
