use crate::id::{Lsn, PageId};
use crate::pager::log::WalSync;
use crate::pager::page::PageMeta;
use crate::runtime::{Atomic, File, Runtime};
use anyhow::anyhow;
use indexmap::IndexSet;
use std::io::SeekFrom;
use std::path::Path;

pub(crate) struct FileManager<R: Runtime> {
    main: R::File,
    double_buff: R::File,
    page_size: usize,

    pub(super) stat: Stat<R>,

    n: usize,
    pages: Box<[u8]>,
    pgids: IndexSet<PageId>,
    lsns: Box<[Lsn]>,
}

pub(super) struct Stat<R: Runtime> {
    pub(super) main_bytes_read: R::AtomicU64,
    pub(super) main_bytes_written: R::AtomicU64,
    pub(super) double_buff_bytes_written: R::AtomicU64,
}

impl<R: Runtime> FileManager<R> {
    pub(crate) async fn new(path: &Path, page_size: usize, n: usize) -> anyhow::Result<Self> {
        let main_path = path.join("main");
        let double_buff_path = path.join("dbuff");

        let mut main = R::File::open(&main_path).await?;
        if !main.is_file().await? {
            return Err(anyhow!("db file is not a regular file"));
        }
        let mut double_buff = R::File::open(&double_buff_path).await?;
        if !double_buff.is_file().await? {
            return Err(anyhow!("double buffer file is not a regular file"));
        }

        Self::recover_non_atomic_writes(&mut main, &mut double_buff, page_size).await?;
        Ok(Self {
            main,
            double_buff,
            page_size,

            stat: Stat {
                main_bytes_read: R::AtomicU64::new(0),
                main_bytes_written: R::AtomicU64::new(0),
                double_buff_bytes_written: R::AtomicU64::new(0),
            },

            n,
            pages: vec![0u8; page_size * n].into_boxed_slice(),
            pgids: IndexSet::with_capacity(n),
            lsns: vec![Lsn::new(0); n].into_boxed_slice(),
        })
    }

    async fn recover_non_atomic_writes(
        f: &mut R::File,
        dbuff: &mut R::File,
        page_size: usize,
    ) -> anyhow::Result<()> {
        let size = dbuff.len().await?;
        let count = (size as usize) / page_size;

        let mut buff = vec![0u8; page_size * count];
        dbuff.seek(SeekFrom::Start(0)).await?;
        dbuff.read_exact(&mut buff).await?;

        for i in 0..count {
            let buff = &buff[i * page_size..(i + 1) * page_size];
            let Some(meta) = PageMeta::decode(buff)? else {
                continue;
            };
            Self::write_page_no_sync(f, page_size as u64, meta.id(), buff).await?;
        }

        f.sync().await?;
        Ok(())
    }

    async fn write_page_no_sync(
        f: &mut R::File,
        page_size: u64,
        id: PageId,
        buff: &[u8],
    ) -> anyhow::Result<()> {
        let file_size = f.len().await?;
        let min_size = id.get() * page_size + page_size;
        if min_size > file_size {
            f.truncate(id.get() * page_size + page_size).await?;
        }
        f.seek(SeekFrom::Start(id.get() * page_size)).await?;
        f.write_all(buff).await?;
        Ok(())
    }

    pub(crate) async fn spill(
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
                self.sync(wal).await?;
            }
            let i = self.pgids.len();
            self.pgids.insert(meta.id);
            i
        };
        self.pages[index * self.page_size..(index + 1) * self.page_size].copy_from_slice(buff);
        self.lsns[index] = meta.lsn;

        Ok(())
    }

    pub(crate) async fn sync(&mut self, wal: &impl WalSync) -> anyhow::Result<()> {
        if let Some(max_lsn) = (0..self.pgids.len()).map(|i| self.lsns[i]).max() {
            wal.sync(max_lsn).await?;
        }

        // WARNING: it is important to write the double buffer after the wal is flushed to the
        // point where all the page that will be flushed have their logs written to the wal.
        self.double_buff.truncate(0).await?;
        self.double_buff.seek(SeekFrom::Start(0)).await?;
        self.double_buff.write_all(&self.pages).await?;
        self.double_buff.sync().await?;
        self.stat
            .double_buff_bytes_written
            .fetch_add(self.pages.len() as u64)
            .await;

        for (i, pgid) in self.pgids.iter().enumerate() {
            // TODO: maybe we can use vectorized write to write them all in one single syscall
            let page_size = self.page_size as u64;
            let file_size = self.main.len().await?;
            let min_size = pgid.get() * page_size + page_size;
            if min_size > file_size {
                self.main
                    .truncate(pgid.get() * page_size + page_size)
                    .await?;
            }
            self.main
                .seek(SeekFrom::Start(pgid.get() * page_size))
                .await?;
            let buff = &self.pages[i * self.page_size..(i + 1) * self.page_size];
            self.main.write_all(buff).await?;
            self.stat.main_bytes_written.fetch_add(buff.len() as u64).await;
        }
        self.main.sync().await?;
        self.pgids.clear();

        Ok(())
    }

    pub(crate) async fn get(&mut self, pgid: PageId, buff: &mut [u8]) -> anyhow::Result<bool> {
        if let Some((i, _)) = self.pgids.get_full(&pgid) {
            buff.copy_from_slice(&self.pages[i * self.page_size..(i + 1) * self.page_size]);
            Ok(true)
        } else {
            let page_size = self.page_size as u64;
            let file_size = self.main.len().await?;
            let min_size = pgid.get() * page_size + page_size;
            if min_size > file_size {
                return Ok(false);
            }
            self.main
                .seek(SeekFrom::Start(pgid.get() * page_size))
                .await?;
            self.main.read_exact(buff).await?;
            self.stat.main_bytes_read.fetch_add(buff.len() as u64).await;
            Ok(true)
        }
    }
}
