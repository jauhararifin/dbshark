use crate::file_lock::FileLock;
use crate::id::{Lsn, PageId};
use crate::pager_v2::page::PageMeta;
use crate::wal::Wal;
use anyhow::anyhow;
use indexmap::IndexSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub(crate) struct FileManager {
    main: File,
    double_buff: File,
    page_size: usize,

    wal: Arc<Wal>,

    n: usize,
    pages: Box<[u8]>,
    pgids: IndexSet<PageId>,
    lsns: Box<[Lsn]>,
}

impl FileManager {
    pub(crate) fn new(
        path: &Path,
        wal: Arc<Wal>,
        page_size: usize,
        n: usize,
    ) -> anyhow::Result<Self> {
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

            wal,

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

    pub(crate) fn spill(&mut self, meta: &PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        assert_eq!(self.page_size, buff.len());
        meta.encode(buff)?;

        let index = if let Some((i, _)) = self.pgids.get_full(&meta.id) {
            i
        } else {
            let is_full = self.pgids.len() >= self.n;
            if is_full {
                self.flush_and_clear_flushing_pages()?;
            }
            let i = self.pgids.len();
            self.pgids.insert(meta.id);
            i
        };
        self.pages[index * self.page_size..(index + 1) * self.page_size].copy_from_slice(buff);
        self.lsns[index] = meta.lsn;

        Ok(())
    }

    fn flush_and_clear_flushing_pages(&mut self) -> anyhow::Result<()> {
        self.double_buff.set_len(0)?;
        self.double_buff.seek(SeekFrom::Start(0))?;
        self.double_buff.write_all(&self.pages)?;
        self.double_buff.sync_all()?;

        if let Some(max_lsn) = (0..self.pgids.len()).map(|i| self.lsns[i]).max() {
            self.wal.sync(max_lsn)?;
        }

        // TODO: maybe we can use vectorized write to write them all in one single syscall
        for (i, pgid) in self.pgids.iter().enumerate() {
            write_page(
                &mut self.main,
                *pgid,
                self.page_size,
                &self.pages[i * self.page_size..(i + 1) * self.page_size],
            )?;
        }
        self.main.sync_all()?;
        self.pgids.clear();

        Ok(())
    }
}

fn write_page(f: &mut File, id: PageId, page_size: usize, buff: &[u8]) -> anyhow::Result<()> {
    let page_size = page_size as u64;
    let file_size = f.metadata()?.len();
    let min_size = id.get() * page_size + page_size;
    if min_size > file_size {
        f.set_len(id.get() * page_size + page_size)?;
    }
    f.seek(SeekFrom::Start(id.get() * page_size))?;
    f.write_all(buff)?;
    Ok(())
}
