use super::buffer::{ReadFrame, WriteFrame};
use super::log::LogContext;
use crate::bins::SliceExt;
use crate::content::{Bytes, Content};
use crate::id::{Lsn, LsnExt, PageId, PageIdExt, TxId};
use anyhow::anyhow;
use std::ops::Range;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PageMeta {
    pub(crate) id: PageId,
    pub(crate) kind: PageKind,
    pub(crate) lsn: Lsn,
    pub(crate) dirty: bool,
}

impl PageMeta {
    pub(crate) fn empty(pgid: PageId) -> Self {
        Self {
            id: pgid,
            kind: PageKind::None,
            lsn: Lsn::new(0),
            dirty: false,
        }
    }

    pub(crate) fn init(pgid: PageId, lsn: Lsn) -> Self {
        Self {
            id: pgid,
            kind: PageKind::None,
            lsn,
            dirty: false,
        }
    }
}

const PAGE_HEADER_SIZE: usize = 24;
const PAGE_HEADER_VERSION_RANGE: Range<usize> = 0..2;
const PAGE_HEADER_KIND_INDEX: usize = 2;
const PAGE_HEADER_PAGE_LSN_RANGE: Range<usize> = 8..16;
const PAGE_HEADER_PAGE_ID_RANGE: Range<usize> = 16..24;

const PAGE_FOOTER_SIZE: usize = 8;
const PAGE_FOOTER_CHECKSUM_RANGE: Range<usize> = 0..8;

const INTERIOR_PAGE_HEADER_SIZE: usize = 16;
const INTERIOR_HEADER_LAST_RANGE: Range<usize> = 0..8;
const INTERIOR_HEADER_COUNT_RANGE: Range<usize> = 8..10;
const INTERIOR_HEADER_OFFSET_RANGE: Range<usize> = 10..12;

const INTERIOR_CELL_SIZE: usize = 24;
const INTERIOR_CELL_PTR_RANGE: Range<usize> = 0..8;
const INTERIOR_CELL_OVERFLOW_RANGE: Range<usize> = 8..16;
const INTERIOR_CELL_KEY_SIZE_RANGE: Range<usize> = 16..20;
const INTERIOR_CELL_OFFSET_RANGE: Range<usize> = 20..22;
const INTERIOR_CELL_SIZE_RANGE: Range<usize> = 22..24;

const LEAF_PAGE_HEADER_SIZE: usize = 16;
const LEAF_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const LEAF_HEADER_COUNT_RANGE: Range<usize> = 8..10;
const LEAF_HEADER_OFFSET_RANGE: Range<usize> = 10..12;

const LEAF_CELL_SIZE: usize = 24;
const LEAF_CELL_OVERFLOW_RANGE: Range<usize> = 0..8;
const LEAF_CELL_KEY_SIZE_RANGE: Range<usize> = 8..12;
const LEAF_CELL_VAL_SIZE_RANGE: Range<usize> = 12..16;
const LEAF_CELL_OFFSET_RANGE: Range<usize> = 16..18;
const LEAF_CELL_SIZE_RANGE: Range<usize> = 18..20;

const OVERFLOW_PAGE_HEADER_SIZE: usize = 16;
const OVERFLOW_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const OVERFLOW_HEADER_SIZE_RANGE: Range<usize> = 8..10;

const FREELIST_PAGE_HEADER_SIZE: usize = 16;
const FREELIST_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const FREELIST_HEADER_COUNT_RANGE: Range<usize> = 8..10;

macro_rules! const_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

#[allow(dead_code)]
const fn range_size(range: Range<usize>) -> usize {
    range.end - range.start
}

const_assert!(PAGE_HEADER_VERSION_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_VERSION_RANGE) == 2);
const_assert!(PAGE_HEADER_KIND_INDEX < PAGE_HEADER_SIZE);
const_assert!(PAGE_HEADER_PAGE_LSN_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_PAGE_LSN_RANGE) == 8);
const_assert!(PAGE_HEADER_PAGE_ID_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_PAGE_ID_RANGE) == 8);

const_assert!(PAGE_FOOTER_CHECKSUM_RANGE.end <= PAGE_FOOTER_SIZE);
const_assert!(range_size(PAGE_FOOTER_CHECKSUM_RANGE) == 8);

const_assert!(INTERIOR_HEADER_LAST_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_LAST_RANGE) == 8);
const_assert!(INTERIOR_HEADER_COUNT_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_COUNT_RANGE) == 2);
const_assert!(INTERIOR_HEADER_OFFSET_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_OFFSET_RANGE) == 2);

const_assert!(INTERIOR_CELL_OVERFLOW_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_OVERFLOW_RANGE) == 8);
const_assert!(INTERIOR_CELL_PTR_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_PTR_RANGE) == 8);
const_assert!(INTERIOR_CELL_KEY_SIZE_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_KEY_SIZE_RANGE) == 4);
const_assert!(INTERIOR_CELL_OFFSET_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_OFFSET_RANGE) == 2);
const_assert!(INTERIOR_CELL_SIZE_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_SIZE_RANGE) == 2);

const_assert!(LEAF_CELL_OVERFLOW_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_OVERFLOW_RANGE) == 8);
const_assert!(LEAF_CELL_KEY_SIZE_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_KEY_SIZE_RANGE) == 4);
const_assert!(LEAF_CELL_VAL_SIZE_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_VAL_SIZE_RANGE) == 4);
const_assert!(LEAF_HEADER_NEXT_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_NEXT_RANGE) == 8);
const_assert!(LEAF_HEADER_COUNT_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_COUNT_RANGE) == 2);
const_assert!(LEAF_HEADER_OFFSET_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_OFFSET_RANGE) == 2);

const_assert!(LEAF_CELL_OFFSET_RANGE.end <= LEAF_CELL_SIZE);
const_assert!(range_size(LEAF_CELL_OFFSET_RANGE) == 2);
const_assert!(LEAF_CELL_SIZE_RANGE.end <= LEAF_CELL_SIZE);
const_assert!(range_size(LEAF_CELL_SIZE_RANGE) == 2);

const_assert!(OVERFLOW_HEADER_NEXT_RANGE.end <= OVERFLOW_PAGE_HEADER_SIZE);
const_assert!(range_size(OVERFLOW_HEADER_NEXT_RANGE) == 8);
const_assert!(OVERFLOW_HEADER_SIZE_RANGE.end <= OVERFLOW_PAGE_HEADER_SIZE);
const_assert!(range_size(OVERFLOW_HEADER_SIZE_RANGE) == 2);

const_assert!(FREELIST_HEADER_NEXT_RANGE.end <= FREELIST_PAGE_HEADER_SIZE);
const_assert!(range_size(FREELIST_HEADER_NEXT_RANGE) == 8);
const_assert!(FREELIST_HEADER_COUNT_RANGE.end <= FREELIST_PAGE_HEADER_SIZE);
const_assert!(range_size(FREELIST_HEADER_COUNT_RANGE) == 2);

impl PageMeta {
    pub(crate) fn encode(&self, buff: &mut [u8]) -> anyhow::Result<()> {
        let page_size = buff.len();
        let header = &mut buff[..PAGE_HEADER_SIZE];

        header[PAGE_HEADER_VERSION_RANGE].fill(0);

        let kind = match self.kind {
            PageKind::None => 0,
            PageKind::Interior { .. } => 1,
            PageKind::Leaf { .. } => 2,
            PageKind::Overflow { .. } => 3,
            PageKind::Freelist { .. } => 4,
        };
        header[PAGE_HEADER_KIND_INDEX] = kind;

        header[PAGE_HEADER_PAGE_LSN_RANGE].copy_from_slice(&self.lsn.to_be_bytes());
        header[PAGE_HEADER_PAGE_ID_RANGE].copy_from_slice(&self.id.to_be_bytes());

        let payload_buff = buff.payload_mut();
        self.kind.encode(payload_buff);

        let checksum = crc64::crc64(0x1d0f, &buff[..page_size - PAGE_FOOTER_SIZE]);
        let footer = &mut buff[page_size - PAGE_FOOTER_SIZE..];
        footer[PAGE_FOOTER_CHECKSUM_RANGE].copy_from_slice(&checksum.to_be_bytes());

        Ok(())
    }

    pub(crate) fn decode(buff: &[u8]) -> anyhow::Result<Option<Self>> {
        let page_size = buff.len();
        let header = &buff[..PAGE_HEADER_SIZE];
        let footer = &buff[page_size - PAGE_FOOTER_SIZE..];
        let payload = buff.payload();

        let buff_checksum = &footer[PAGE_FOOTER_CHECKSUM_RANGE];
        let buff_version = &header[PAGE_HEADER_VERSION_RANGE];
        let buff_kind = &header[PAGE_HEADER_KIND_INDEX];
        let buff_page_lsn = &header[PAGE_HEADER_PAGE_LSN_RANGE];
        let buff_page_id = &header[PAGE_HEADER_PAGE_ID_RANGE];
        let buff_checksum_content = &buff[..page_size - PAGE_FOOTER_SIZE];

        let checksum = crc64::crc64(0x1d0f, buff_checksum_content);
        let page_sum = buff_checksum.read_u64();
        if checksum != page_sum {
            return Ok(None);
        }
        let version = buff_version.read_u16();
        if version != 0 {
            return Err(anyhow!("page version {} is not supported", version));
        }

        let Some(page_lsn) = Lsn::from_be_bytes(buff_page_lsn.try_into().unwrap()) else {
            return Err(anyhow!("found an empty lsn field when decoding page",));
        };
        let Some(page_id) = PageId::from_be_bytes(buff_page_id.try_into().unwrap()) else {
            return Err(anyhow!("found an empty page_id field when decoding page",));
        };

        let kind = PageKind::decode(*buff_kind, payload)?;

        Ok(Some(Self {
            id: page_id,
            kind,
            lsn: page_lsn,
            dirty: false,
        }))
    }

    #[inline]
    pub(crate) fn id(&self) -> PageId {
        self.id
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PageKind {
    None,
    Interior(InteriorKind),
    Leaf(LeafKind),
    Overflow(OverflowKind),
    Freelist(FreelistKind),
}

impl PageKind {
    fn encode(&self, payload: &mut [u8]) {
        match self {
            Self::None => (),
            Self::Interior(kind) => kind.encode(payload),
            Self::Leaf(kind) => kind.encode(payload),
            Self::Overflow(kind) => kind.encode(payload),
            Self::Freelist(kind) => kind.encode(payload),
        }
    }

    fn decode(kind: u8, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(match kind {
            0 => Self::None,
            1 => Self::Interior(InteriorKind::decode(payload)?),
            2 => Self::Leaf(LeafKind::decode(payload)?),
            3 => Self::Overflow(OverflowKind::decode(payload)?),
            4 => Self::Freelist(FreelistKind::decode(payload)?),
            _ => return Err(anyhow!("page kind {kind} is not recognized")),
        })
    }

    #[inline]
    fn interior(&self) -> &InteriorKind {
        let Self::Interior(kind) = self else {
            panic!("page is not an interior");
        };
        kind
    }

    #[inline]
    fn interior_mut(&mut self) -> &mut InteriorKind {
        let Self::Interior(ref mut kind) = self else {
            panic!("page is not an interior");
        };
        kind
    }

    #[inline]
    fn leaf(&self) -> &LeafKind {
        let Self::Leaf(kind) = self else {
            panic!("page is not a leaf");
        };
        kind
    }

    #[inline]
    fn leaf_mut(&mut self) -> &mut LeafKind {
        let Self::Leaf(kind) = self else {
            panic!("page is not a leaf");
        };
        kind
    }

    #[inline]
    fn overflow(&self) -> &OverflowKind {
        let Self::Overflow(kind) = self else {
            panic!("page is not an overflow");
        };
        kind
    }

    #[inline]
    fn overflow_mut(&mut self) -> &mut OverflowKind {
        let Self::Overflow(kind) = self else {
            panic!("page is not an overflow");
        };
        kind
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InteriorKind {
    count: usize,
    offset: usize,
    remaining: usize,
    last: PageId,
}

impl InteriorKind {
    fn encode(&self, payload: &mut [u8]) {
        let header = &mut payload[..INTERIOR_PAGE_HEADER_SIZE];
        header[INTERIOR_HEADER_LAST_RANGE].copy_from_slice(&self.last.to_be_bytes());
        header[INTERIOR_HEADER_COUNT_RANGE].copy_from_slice(&(self.count as u16).to_be_bytes());
        header[INTERIOR_HEADER_OFFSET_RANGE].copy_from_slice(&(self.offset as u16).to_be_bytes());
    }

    fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let header = &payload[..INTERIOR_PAGE_HEADER_SIZE];
        let buff_last = &header[INTERIOR_HEADER_LAST_RANGE];
        let buff_count = &header[INTERIOR_HEADER_COUNT_RANGE];
        let buff_offset = &header[INTERIOR_HEADER_OFFSET_RANGE];

        let Some(last) = PageId::from_be_bytes(buff_last.try_into().unwrap()) else {
            return Err(anyhow!("got zero last ptr on interior page"));
        };
        let count = buff_count.read_u16();
        let offset = buff_offset.read_u16();

        let mut remaining = payload.len() - INTERIOR_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell = &payload[get_interior_cell_range(i as usize)];
            if cell[INTERIOR_CELL_PTR_RANGE].read_u64() == 0 {
                return Err(anyhow!("got zero ptr on interior page cell={i}"));
            }
            remaining -= INTERIOR_CELL_SIZE + cell[INTERIOR_CELL_SIZE_RANGE].read_u16() as usize;
        }

        Ok(InteriorKind {
            count: count as usize,
            offset: offset as usize,
            remaining,
            last,
        })
    }
}

#[inline]
pub(super) const fn get_interior_cell_range(index: usize) -> Range<usize> {
    let cell_offset = INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * index;
    cell_offset..cell_offset + INTERIOR_CELL_SIZE
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LeafKind {
    count: usize,
    offset: usize,
    remaining: usize,
    next: Option<PageId>,
}

impl LeafKind {
    fn encode(&self, payload: &mut [u8]) {
        let header = &mut payload[..LEAF_PAGE_HEADER_SIZE];
        header[LEAF_HEADER_NEXT_RANGE].copy_from_slice(&self.next.to_be_bytes());
        header[LEAF_HEADER_COUNT_RANGE].copy_from_slice(&(self.count as u16).to_be_bytes());
        header[LEAF_HEADER_OFFSET_RANGE].copy_from_slice(&(self.offset as u16).to_be_bytes());
    }

    fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let header = &payload[..LEAF_PAGE_HEADER_SIZE];
        let buff_next = &header[LEAF_HEADER_NEXT_RANGE];
        let buff_count = &header[LEAF_HEADER_COUNT_RANGE];
        let buff_offset = &header[LEAF_HEADER_OFFSET_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = buff_count.read_u16();
        let offset = buff_offset.read_u16();

        let mut remaining = payload.len() - LEAF_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell = &payload[get_leaf_cell_range(i as usize)];
            remaining -= LEAF_CELL_SIZE + cell[LEAF_CELL_SIZE_RANGE].read_u16() as usize;
        }

        Ok(LeafKind {
            count: count as usize,
            offset: offset as usize,
            remaining,
            next,
        })
    }
}

#[inline]
const fn get_leaf_cell_range(index: usize) -> Range<usize> {
    let cell_offset = LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * index;
    cell_offset..cell_offset + LEAF_CELL_SIZE
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OverflowKind {
    next: Option<PageId>,
    size: usize,
}

impl OverflowKind {
    fn encode(&self, payload: &mut [u8]) {
        let header = &mut payload[..OVERFLOW_PAGE_HEADER_SIZE];
        header[OVERFLOW_HEADER_NEXT_RANGE].copy_from_slice(&self.next.to_be_bytes());
        header[OVERFLOW_HEADER_SIZE_RANGE].copy_from_slice(&(self.size as u16).to_be_bytes());
    }

    fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let header = &payload[..OVERFLOW_PAGE_HEADER_SIZE];
        let buff_next = &header[OVERFLOW_HEADER_NEXT_RANGE];
        let buff_size = &header[OVERFLOW_HEADER_SIZE_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let size = buff_size.read_u16();

        Ok(OverflowKind {
            next,
            size: size as usize,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FreelistKind {
    pub(crate) next: Option<PageId>,
    pub(crate) count: usize,
}

impl FreelistKind {
    fn encode(&self, payload: &mut [u8]) {
        let header = &mut payload[..FREELIST_PAGE_HEADER_SIZE];
        header[FREELIST_HEADER_NEXT_RANGE].copy_from_slice(&self.next.to_be_bytes());
        header[FREELIST_HEADER_COUNT_RANGE].copy_from_slice(&(self.count as u16).to_be_bytes());
    }

    fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let header = &payload[..FREELIST_PAGE_HEADER_SIZE];
        let buff_next = &header[FREELIST_HEADER_NEXT_RANGE];
        let buff_count = &header[FREELIST_HEADER_COUNT_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = buff_count.read_u16();

        Ok(FreelistKind {
            next,
            count: count as usize,
        })
    }
}

pub(crate) struct PageInternal<'a> {
    pub(crate) txid: TxId,
    pub(crate) meta: &'a PageMeta,
    pub(crate) buffer: &'a [u8],
}

pub(crate) trait PageOps<'a>: Sized {
    fn internal(&self) -> PageInternal;

    #[inline]
    fn is_none(&self) -> bool {
        matches!(self.internal().meta.kind, PageKind::None)
    }

    #[inline]
    fn id(&self) -> PageId {
        self.internal().meta.id()
    }

    #[inline]
    fn lsn(&self) -> Lsn {
        self.internal().meta.lsn
    }

    #[inline]
    fn is_interior(&self) -> bool {
        matches!(&self.internal().meta.kind, PageKind::Interior { .. })
    }

    #[inline]
    fn page_lsn(&self) -> Lsn {
        self.internal().meta.lsn
    }

    #[inline]
    fn into_interior(self) -> Option<InteriorPageRead<Self>> {
        if let PageKind::Interior(..) = &self.internal().meta.kind {
            Some(InteriorPageRead(self))
        } else {
            None
        }
    }

    #[inline]
    fn into_leaf(self) -> Option<LeafPageRead<Self>> {
        if let PageKind::Leaf(..) = &self.internal().meta.kind {
            Some(LeafPageRead(self))
        } else {
            None
        }
    }

    #[inline]
    fn into_overflow(self) -> Option<OverflowPageRead<Self>> {
        if let PageKind::Overflow(..) = &self.internal().meta.kind {
            Some(OverflowPageRead(self))
        } else {
            None
        }
    }
}

pub(crate) struct PageInternalWrite<'a> {
    pub(crate) txid: TxId,
    pub(crate) meta: &'a mut PageMeta,
    pub(crate) buffer: &'a mut [u8],
}

pub(crate) trait PageWriteOps<'a>: PageOps<'a> {
    fn internal_mut(&mut self) -> PageInternalWrite;

    fn init_interior(
        mut self,
        ctx: LogContext<'_>,
        last: PageId,
    ) -> anyhow::Result<InteriorPageWrite<Self>> {
        let page_size = self.internal().buffer.len();
        if let PageKind::None = self.internal().meta.kind {
            let pgid = self.id();
            let internal = self.internal_mut();
            internal.meta.lsn = ctx.record_interior_init(internal.txid, pgid, last)?;
            internal.meta.dirty = true;
            internal.meta.kind = PageKind::Interior(InteriorKind {
                count: 0,
                offset: page_size - PAGE_FOOTER_SIZE,
                remaining: page_size
                    - PAGE_HEADER_SIZE
                    - PAGE_FOOTER_SIZE
                    - INTERIOR_PAGE_HEADER_SIZE,
                last,
            });
        }

        let Some(interior) = self.into_write_interior() else {
            return Err(anyhow!(
                "cannot init interior page because page is not an empty page nor interior page",
            ));
        };

        Ok(interior)
    }

    fn into_write_interior(self) -> Option<InteriorPageWrite<Self>> {
        if let PageKind::Interior { .. } = &self.internal().meta.kind {
            Some(InteriorPageWrite(self))
        } else {
            None
        }
    }

    fn set_interior(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<InteriorPageWrite<Self>> {
        assert!(
            matches!(self.internal().meta.kind, PageKind::None),
            "page is not empty"
        );

        let pgid = self.id();
        let internal = self.internal_mut();
        internal.meta.lsn = ctx.record_interior_set(internal.txid, pgid, Bytes::new(payload))?;
        internal.meta.dirty = true;

        internal.meta.kind = PageKind::Interior(InteriorKind::decode(payload)?);
        internal.buffer.payload_mut().copy_from_slice(payload);

        Ok(self
            .into_write_interior()
            .expect("the page should be an interior now"))
    }

    fn init_leaf(mut self, ctx: LogContext<'_>) -> anyhow::Result<LeafPageWrite<Self>> {
        let page_size = self.internal().buffer.len();
        if let PageKind::None = self.internal().meta.kind {
            let pgid = self.id();
            let internal = self.internal_mut();
            internal.meta.lsn = ctx.record_leaf_init(internal.txid, pgid)?;
            internal.meta.dirty = true;
            internal.meta.kind = PageKind::Leaf(LeafKind {
                count: 0,
                offset: page_size - PAGE_FOOTER_SIZE,
                remaining: page_size - PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE - LEAF_PAGE_HEADER_SIZE,
                next: None,
            });
        }

        let Some(leaf) = self.into_write_leaf() else {
            return Err(anyhow!(
                "cannot init leaf page because page is not an empty page nor leaf page",
            ));
        };

        Ok(leaf)
    }

    fn set_leaf(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<LeafPageWrite<Self>> {
        assert!(
            matches!(self.internal().meta.kind, PageKind::None),
            "page is not empty"
        );

        let pgid = self.id();
        let internal = self.internal_mut();
        internal.meta.lsn = ctx.record_leaf_set(internal.txid, pgid, Bytes::new(payload))?;
        internal.meta.dirty = true;

        internal.meta.kind = PageKind::Leaf(LeafKind::decode(payload)?);
        internal.buffer.payload_mut().copy_from_slice(payload);

        Ok(self
            .into_write_leaf()
            .expect("the page should be a leaf now"))
    }

    fn into_write_leaf(self) -> Option<LeafPageWrite<Self>> {
        if let PageKind::Leaf(..) = &self.internal().meta.kind {
            Some(LeafPageWrite(self))
        } else {
            None
        }
    }

    fn init_overflow(mut self, ctx: LogContext<'_>) -> anyhow::Result<OverflowPageWrite<Self>> {
        if let PageKind::None = self.internal().meta.kind {
            let pgid = self.id();
            let internal = self.internal_mut();
            internal.meta.lsn = ctx.record_overflow_init(internal.txid, pgid)?;
            internal.meta.dirty = true;
            internal.meta.kind = PageKind::Overflow(OverflowKind {
                next: None,
                size: 0,
            });
        }

        let Some(overflow) = self.into_write_overflow() else {
            return Err(anyhow!(
                "cannot init overflow page because page is not an empty page nor overflow page",
            ));
        };

        Ok(overflow)
    }

    fn set_overflow(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<OverflowPageWrite<Self>> {
        assert!(
            matches!(self.internal().meta.kind, PageKind::None),
            "page is not empty"
        );
        let LogContext::Redo(lsn) = ctx else {
            panic!("set_overflow only can be used for redo-ing wal");
        };

        let internal = self.internal_mut();
        internal.meta.lsn = lsn;
        internal.meta.dirty = true;
        internal.meta.kind = PageKind::Overflow(OverflowKind::decode(payload)?);
        internal.buffer.payload_mut().copy_from_slice(payload);

        Ok(self
            .into_write_overflow()
            .expect("the page should be an overflow now"))
    }

    #[inline]
    fn into_write_overflow(self) -> Option<OverflowPageWrite<Self>> {
        if let PageKind::Overflow(..) = &self.internal().meta.kind {
            Some(OverflowPageWrite(self))
        } else {
            None
        }
    }
}

pub(crate) struct InteriorPageRead<T>(T);

pub(crate) trait InteriorPage<'a>: PageOps<'a> {
    #[inline]
    fn last(&self) -> PageId {
        self.internal().meta.kind.interior().last
    }

    #[inline]
    fn might_split(&self) -> bool {
        let kind = self.internal().meta.kind.interior();
        let page_size = self.internal().buffer.len();

        let payload_size =
            page_size - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - INTERIOR_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let remaining = if kind.remaining < INTERIOR_CELL_SIZE {
            0
        } else {
            kind.remaining - INTERIOR_CELL_SIZE
        };
        remaining < min_content_not_overflow
    }

    #[inline]
    fn count(&self) -> usize {
        self.internal().meta.kind.interior().count
    }

    #[inline]
    fn get(&self, index: usize) -> InteriorCell<'_> {
        let payload = &self.internal().buffer.payload();
        get_interior_cell(payload, index)
    }
}

impl<'a, T> PageOps<'a> for InteriorPageRead<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> InteriorPage<'a> for InteriorPageRead<T> where T: PageOps<'a> {}

fn get_interior_cell(payload: &[u8], index: usize) -> InteriorCell<'_> {
    let cell = &payload[get_interior_cell_range(index)];
    let offset = cell[INTERIOR_CELL_OFFSET_RANGE].read_u16() as usize;
    let size = cell[INTERIOR_CELL_SIZE_RANGE].read_u16() as usize;
    let offset = offset - PAGE_HEADER_SIZE;
    let raw = &payload[offset..offset + size];
    InteriorCell { cell, raw }
}

pub(crate) struct InteriorCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

impl<'a> InteriorCell<'a> {
    #[inline]
    pub(crate) fn raw(&self) -> &'a [u8] {
        self.raw
    }

    #[inline]
    pub(crate) fn ptr(&self) -> PageId {
        PageId::from_be_bytes(self.cell[INTERIOR_CELL_PTR_RANGE].try_into().unwrap()).unwrap()
    }

    #[inline]
    pub(crate) fn key_size(&self) -> usize {
        self.cell[INTERIOR_CELL_KEY_SIZE_RANGE].read_u32() as usize
    }

    #[inline]
    pub(crate) fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[INTERIOR_CELL_OVERFLOW_RANGE].try_into().unwrap())
    }
}

pub(crate) struct InteriorPageWrite<T>(T);

impl<'a, T> PageOps<'a> for InteriorPageWrite<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> PageWriteOps<'a> for InteriorPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    #[inline]
    fn internal_mut(&mut self) -> PageInternalWrite {
        self.0.internal_mut()
    }
}

impl<'a, T> InteriorPage<'a> for InteriorPageWrite<T> where T: PageOps<'a> {}

impl<'a, T> InteriorPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<T> {
        let pgid = self.id();
        let internal = self.internal_mut();
        internal.meta.encode(internal.buffer)?;

        let payload = Bytes::new(internal.buffer.payload());
        internal.meta.lsn = ctx.record_interior_reset(internal.txid, pgid, payload)?;
        internal.meta.dirty = true;
        internal.meta.kind = PageKind::None;
        Ok(self.0)
    }

    pub(crate) fn set_last(&mut self, ctx: LogContext<'_>, new_last: PageId) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let PageKind::Interior(ref mut kind) = internal.meta.kind else {
            unreachable!();
        };
        let old_last = kind.last;
        internal.meta.lsn =
            ctx.record_interior_set_last(internal.txid, pgid, new_last, old_last)?;
        internal.meta.dirty = true;
        kind.last = new_last;
        Ok(())
    }

    pub(crate) fn set_cell_ptr(
        &mut self,
        ctx: LogContext<'_>,
        index: usize,
        ptr: PageId,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let payload = internal.buffer.payload_mut();
        let cell = &mut payload[get_interior_cell_range(index)];
        let old_ptr =
            PageId::from_be_bytes(cell[INTERIOR_CELL_PTR_RANGE].try_into().unwrap()).unwrap();
        if old_ptr == ptr {
            return Ok(());
        }
        internal.meta.lsn =
            ctx.record_interior_set_cell_ptr(internal.txid, pgid, index, ptr, old_ptr)?;
        internal.meta.dirty = true;
        cell[INTERIOR_CELL_PTR_RANGE].copy_from_slice(&ptr.to_be_bytes());
        Ok(())
    }

    pub(crate) fn set_cell_overflow(
        &mut self,
        ctx: LogContext<'_>,
        index: usize,
        overflow_pgid: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let payload = internal.buffer.payload_mut();
        let cell = &mut payload[get_interior_cell_range(index)];
        let old_overflow =
            PageId::from_be_bytes(cell[INTERIOR_CELL_OVERFLOW_RANGE].try_into().unwrap());
        if old_overflow == overflow_pgid {
            return Ok(());
        }
        internal.meta.lsn = ctx.record_interior_set_cell_overflow(
            internal.txid,
            pgid,
            index,
            overflow_pgid,
            old_overflow,
        )?;
        internal.meta.dirty = true;
        cell[INTERIOR_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow_pgid.to_be_bytes());
        Ok(())
    }

    pub(crate) fn insert_cell(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        cell: InteriorCell,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let mut internal = self.internal_mut();
        log::debug!(
            "insert_interior_cell {pgid:?} kind={:?} page_lsn={:?} i={i} cell_raw_len={:?}",
            internal.meta.kind,
            internal.meta.lsn,
            cell.raw().len(),
        );

        let kind = internal.meta.kind.interior();
        let raw = cell.raw();
        assert!(
            raw.len() + INTERIOR_CELL_SIZE <= kind.remaining,
            "insert cell only called in the context of moving a splitted page to a new page, so it should always fit",
        );

        let reserved_offset = Self::insert_cell_meta(
            &mut internal,
            i,
            cell.ptr(),
            cell.overflow(),
            cell.key_size(),
            cell.raw().len(),
        );
        Bytes::new(cell.raw())
            .put(&mut internal.buffer[reserved_offset..reserved_offset + raw.len()])?;

        internal.meta.lsn = ctx.record_interior_insert(
            internal.txid,
            pgid,
            i,
            Bytes::new(&internal.buffer[reserved_offset..reserved_offset + raw.len()]),
            cell.ptr(),
            cell.key_size(),
            cell.overflow(),
        )?;
        internal.meta.dirty = true;

        let internal = self.internal();
        log::debug!(
            "insert_interior_cell_finish {pgid:?} kind={:?} page_lsn={:?} i={i}",
            internal.meta.kind,
            internal.meta.lsn,
        );
        Ok(())
    }

    pub(crate) fn insert_content(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        content: &mut impl Content,
        key_size: usize,
        ptr: PageId,
        overflow: Option<PageId>,
    ) -> anyhow::Result<bool> {
        let pgid = self.id();
        let mut internal = self.internal_mut();
        log::debug!(
            "insert_interior_content {pgid:?} kind={:?} page_lsn={:?} i={i} raw_size={}",
            internal.meta.kind,
            internal.meta.lsn,
            content.remaining(),
        );

        let page_size = internal.buffer.len();
        let total_size = content.remaining();
        let payload_size =
            page_size - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - INTERIOR_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let kind = internal.meta.kind.interior_mut();
        if kind.remaining < INTERIOR_CELL_SIZE {
            return Ok(false);
        }
        let remaining = kind.remaining - INTERIOR_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < total_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, total_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let content_offset =
            Self::insert_cell_meta(&mut internal, i, ptr, overflow, key_size, raw_size);
        content.put(&mut internal.buffer[content_offset..content_offset + raw_size])?;

        internal.meta.lsn = ctx.record_interior_insert(
            internal.txid,
            pgid,
            i,
            Bytes::new(&internal.buffer[content_offset..content_offset + raw_size]),
            ptr,
            key_size,
            overflow,
        )?;
        internal.meta.dirty = true;

        log::debug!(
            "insert_interior_content_finish {pgid:?} kind={:?} page_lsn={:?} i={i} raw_size={}",
            internal.meta.kind,
            internal.meta.lsn,
            content.remaining(),
        );
        Ok(true)
    }

    fn insert_cell_meta(
        internal: &mut PageInternalWrite,
        index: usize,
        ptr: PageId,
        overflow: Option<PageId>,
        key_size: usize,
        raw_size: usize,
    ) -> usize {
        let kind = internal.meta.kind.interior();
        let added = INTERIOR_CELL_SIZE + raw_size;
        let current_cell_size =
            PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * kind.count;
        if current_cell_size + added > kind.offset {
            Self::rearrange(internal);
        }

        let kind = internal.meta.kind.interior_mut();
        let shifted = kind.count - index;
        for i in 0..shifted {
            let x = PAGE_HEADER_SIZE
                + INTERIOR_PAGE_HEADER_SIZE
                + INTERIOR_CELL_SIZE * (kind.count - i);
            let (a, b) = internal.buffer.split_at_mut(x);
            b[..INTERIOR_CELL_SIZE].copy_from_slice(&a[a.len() - INTERIOR_CELL_SIZE..]);
        }

        let payload = internal.buffer.payload_mut();
        let cell = &mut payload[get_interior_cell_range(index)];

        kind.offset -= raw_size;
        kind.remaining -= added;
        kind.count += 1;

        cell[INTERIOR_CELL_PTR_RANGE].copy_from_slice(&ptr.to_be_bytes());
        cell[INTERIOR_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow.to_be_bytes());
        cell[INTERIOR_CELL_KEY_SIZE_RANGE].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[INTERIOR_CELL_OFFSET_RANGE].copy_from_slice(&(kind.offset as u16).to_be_bytes());
        cell[INTERIOR_CELL_SIZE_RANGE].copy_from_slice(&(raw_size as u16).to_be_bytes());

        kind.offset
    }

    fn rearrange(internal: &mut PageInternalWrite) {
        use super::MAXIMUM_PAGE_SIZE;
        use std::cell::RefCell;
        std::thread_local! {
            static TEMP_BUFFER: RefCell<[u8; MAXIMUM_PAGE_SIZE]> = RefCell::new([0u8; MAXIMUM_PAGE_SIZE]);
        }

        let kind = internal.meta.kind.interior_mut();

        TEMP_BUFFER.with_borrow_mut(|copied| {
            let count = kind.count;
            let page_size = internal.buffer.len();
            copied.copy_from_slice(&internal.buffer);

            let mut new_offset = page_size - PAGE_FOOTER_SIZE;
            for i in 0..count {
                let copied_cell = get_interior_cell(copied.payload(), i);
                let copied_content = copied_cell.raw();
                new_offset -= copied_content.len();
                internal.buffer[new_offset..new_offset + copied_content.len()]
                    .copy_from_slice(copied_content);

                let payload = internal.buffer.payload_mut();
                let cell = &mut payload[get_interior_cell_range(i)];
                cell[INTERIOR_CELL_OFFSET_RANGE]
                    .copy_from_slice(&(new_offset as u16).to_be_bytes());
            }
            kind.offset = new_offset;
        })
    }

    pub(crate) fn split<F>(&mut self, ctx: LogContext<'_>, f: F) -> anyhow::Result<usize>
    where
        for<'c> F: Fn(InteriorCell<'c>) -> anyhow::Result<()>,
    {
        let pgid = self.id();
        let internal = self.internal();
        log::debug!(
            "interior_split {pgid:?} kind={:?} page_lsn={:?}",
            internal.meta.kind,
            internal.meta.lsn,
        );
        let page_size = self.internal().buffer.len();

        let payload_size =
            page_size - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let half_payload = payload_size / 2;

        let mut cummulative_size = 0;
        let mut n_cells_to_keep = 0;

        let kind = internal.meta.kind.interior();
        for i in 0..kind.count {
            let cell = self.get(i);
            let new_cummulative_size = cummulative_size + cell.raw.len() + INTERIOR_CELL_SIZE;
            if new_cummulative_size >= half_payload {
                n_cells_to_keep = i;
                break;
            }
            cummulative_size = new_cummulative_size;
        }
        assert!(
            n_cells_to_keep < kind.count,
            "there is no point splitting the page if it doesn't move any entries. n_cells_to_keep={n_cells_to_keep} count={}", kind.count,
        );

        let internal = self.internal_mut();
        let kind = internal.meta.kind.interior_mut();
        let txid = internal.txid;
        for i in (n_cells_to_keep..kind.count).rev() {
            let cell = get_interior_cell(internal.buffer.payload(), i);
            internal.meta.lsn = ctx.record_interior_delete(
                txid,
                pgid,
                i,
                Bytes::new(cell.raw()),
                cell.ptr(),
                cell.overflow(),
                cell.key_size(),
            )?;
        }
        internal.meta.dirty = true;

        let original_count = kind.count;
        kind.count = n_cells_to_keep;
        kind.remaining = payload_size - cummulative_size;

        log::debug!(
            "interior_split_finish {pgid:?} kind={:?} page_lsn={:?}",
            internal.meta.kind,
            internal.meta.lsn,
        );

        for i in n_cells_to_keep..original_count {
            let cell = self.get(i);
            f(cell)?;
        }
        Ok(n_cells_to_keep)
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal();
        log::debug!(
            "interior_delete {pgid:?} kind={:?} page_lsn={:?} i={index}",
            internal.meta.kind,
            internal.meta.lsn,
        );

        let cell = self.get(index);
        let content_offset = cell.cell[INTERIOR_CELL_OFFSET_RANGE].read_u16() as usize;
        let content_size = cell.cell[INTERIOR_CELL_SIZE_RANGE].read_u16() as usize;
        let ptr = cell.ptr();
        let overflow = cell.overflow();
        let key_size = cell.key_size();

        let internal = self.internal_mut();
        internal.meta.lsn = ctx.record_interior_delete(
            internal.txid,
            pgid,
            index,
            Bytes::new(&internal.buffer[content_offset..content_offset + content_size]),
            ptr,
            overflow,
            key_size,
        )?;
        internal.meta.dirty = true;

        let PageKind::Interior(ref mut kind) = internal.meta.kind else {
            unreachable!();
        };
        if kind.offset == content_offset {
            kind.offset += content_size;
        }
        kind.remaining += INTERIOR_CELL_SIZE + content_size;
        kind.count -= 1;

        for i in index..kind.count {
            let cell_offset = PAGE_HEADER_SIZE + get_interior_cell_range(i).start;
            let (a, b) = internal
                .buffer
                .split_at_mut(cell_offset + INTERIOR_CELL_SIZE);
            let a_len = a.len();
            a[a_len - INTERIOR_CELL_SIZE..].copy_from_slice(&b[..INTERIOR_CELL_SIZE]);
        }

        log::debug!(
            "interior_delete_finish {pgid:?} kind={kind:?} page_lsn={:?} i={index}",
            internal.meta.lsn,
        );
        Ok(())
    }
}

pub(crate) struct LeafPageRead<T>(T);

pub(crate) trait LeafPage<'a>: PageOps<'a> {
    #[inline]
    fn next(&self) -> Option<PageId> {
        self.internal().meta.kind.leaf().next
    }

    #[inline]
    fn count(&self) -> usize {
        self.internal().meta.kind.leaf().count
    }

    #[inline]
    fn get(&self, index: usize) -> LeafCell<'_> {
        let payload = self.internal().buffer.payload();
        get_leaf_cell(payload, index)
    }
}

impl<'a, T> PageOps<'a> for LeafPageRead<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> LeafPage<'a> for LeafPageRead<T> where T: PageOps<'a> {}

fn get_leaf_cell(payload: &[u8], index: usize) -> LeafCell<'_> {
    let cell = &payload[get_leaf_cell_range(index)];
    let offset = cell[LEAF_CELL_OFFSET_RANGE].read_u16() as usize;
    let size = cell[LEAF_CELL_SIZE_RANGE].read_u16() as usize;
    let offset = offset - PAGE_HEADER_SIZE;
    let raw = &payload[offset..offset + size];
    LeafCell { cell, raw }
}

pub(crate) struct LeafCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

impl<'a> LeafCell<'a> {
    #[inline]
    pub(crate) fn raw(&self) -> &'a [u8] {
        self.raw
    }

    #[inline]
    pub(crate) fn key_size(&self) -> usize {
        self.cell[LEAF_CELL_KEY_SIZE_RANGE].read_u32() as usize
    }

    #[inline]
    pub(crate) fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[LEAF_CELL_OVERFLOW_RANGE].try_into().unwrap())
    }

    #[inline]
    pub(crate) fn val_size(&self) -> usize {
        self.cell[LEAF_CELL_VAL_SIZE_RANGE].read_u32() as usize
    }
}

pub(crate) struct LeafPageWrite<T>(T);

impl<'a, T> PageOps<'a> for LeafPageWrite<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> PageWriteOps<'a> for LeafPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    #[inline]
    fn internal_mut(&mut self) -> PageInternalWrite {
        self.0.internal_mut()
    }
}

impl<'a, T> LeafPage<'a> for LeafPageWrite<T> where T: PageWriteOps<'a> {}

impl<'a, T> LeafPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<T> {
        let pgid = self.id();
        let internal = self.internal_mut();
        internal.meta.encode(internal.buffer)?;

        let payload = Bytes::new(internal.buffer.payload());
        internal.meta.lsn = ctx.record_leaf_reset(internal.txid, pgid, payload)?;
        internal.meta.dirty = true;
        internal.meta.kind = PageKind::None;
        Ok(self.0)
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal();
        log::debug!(
            "leaf_delete {pgid:?} kind={:?} page_lsn={:?} i={index}",
            internal.meta.kind,
            internal.meta.lsn,
        );

        let cell = self.get(index);
        let content_offset = cell.cell[LEAF_CELL_OFFSET_RANGE].read_u16() as usize;
        let content_size = cell.cell[LEAF_CELL_SIZE_RANGE].read_u16() as usize;
        let overflow = cell.overflow();
        let key_size = cell.key_size();
        let val_size = cell.val_size();

        let internal = self.internal_mut();
        internal.meta.lsn = ctx.record_leaf_delete(
            internal.txid,
            pgid,
            index,
            Bytes::new(&internal.buffer[content_offset..content_offset + content_size]),
            overflow,
            key_size,
            val_size,
        )?;
        internal.meta.dirty = true;

        let kind = internal.meta.kind.leaf_mut();
        if kind.offset == content_offset {
            kind.offset += content_size;
        }
        kind.remaining += LEAF_CELL_SIZE + content_size;
        kind.count -= 1;

        for i in index..kind.count {
            let cell_offset = PAGE_HEADER_SIZE + get_leaf_cell_range(i).start;
            let (a, b) = internal.buffer.split_at_mut(cell_offset + LEAF_CELL_SIZE);
            let a_len = a.len();
            a[a_len - LEAF_CELL_SIZE..].copy_from_slice(&b[..LEAF_CELL_SIZE]);
        }

        log::debug!(
            "leaf_delete_finish {pgid:?} kind={kind:?} page_lsn={:?} i={index}",
            internal.meta.lsn,
        );
        Ok(())
    }

    pub(crate) fn set_next(
        &mut self,
        ctx: LogContext<'_>,
        new_next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let kind = internal.meta.kind.leaf_mut();
        let old_next = kind.next;
        if old_next == new_next {
            return Ok(());
        }
        internal.meta.lsn = ctx.record_leaf_set_next(internal.txid, pgid, new_next, old_next)?;
        internal.meta.dirty = true;
        kind.next = new_next;
        Ok(())
    }

    pub(crate) fn set_cell_overflow(
        &mut self,
        ctx: LogContext<'_>,
        index: usize,
        overflow_pgid: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let payload = internal.buffer.payload_mut();
        let cell = &mut payload[get_leaf_cell_range(index)];
        let old_overflow =
            PageId::from_be_bytes(cell[LEAF_CELL_OVERFLOW_RANGE].try_into().unwrap());
        if old_overflow == overflow_pgid {
            return Ok(());
        }
        internal.meta.lsn = ctx.record_leaf_set_cell_overflow(
            internal.txid,
            pgid,
            index,
            overflow_pgid,
            old_overflow,
        )?;
        internal.meta.dirty = true;
        cell[LEAF_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow_pgid.to_be_bytes());
        Ok(())
    }

    pub(crate) fn insert_cell(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        cell: LeafCell,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let mut internal = self.internal_mut();
        log::debug!(
            "insert_leaf_cell {pgid:?} kind={:?} page_lsn={:?} i={i} cell_raw_len={:?}",
            internal.meta.kind,
            internal.meta.lsn,
            cell.raw().len(),
        );

        let PageKind::Leaf(ref kind) = internal.meta.kind else {
            unreachable!();
        };
        let raw = cell.raw();
        assert!(
            raw.len() + LEAF_CELL_SIZE <= kind.remaining,
            "insert cell only called in the context of moving a splitted page to a new page, so it should always fit",
        );

        let reserved_offset = Self::reserve_cell(&mut internal, raw.len());
        internal.buffer[reserved_offset..reserved_offset + raw.len()].copy_from_slice(raw);

        internal.meta.lsn = ctx.record_leaf_insert(
            internal.txid,
            pgid,
            i,
            Bytes::new(cell.raw()),
            cell.overflow(),
            cell.key_size(),
            cell.val_size(),
        )?;
        internal.meta.dirty = true;

        Self::insert_cell_meta(
            &mut internal,
            i,
            cell.overflow(),
            cell.key_size(),
            cell.val_size(),
            raw.len(),
        );

        Ok(())
    }

    fn reserve_cell(internal: &mut PageInternalWrite, raw_size: usize) -> usize {
        let added = LEAF_CELL_SIZE + raw_size;
        let kind = internal.meta.kind.leaf_mut();
        assert!(
            added <= kind.remaining,
            "added cell should be fit in the page"
        );
        let current_cell_size =
            PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * kind.count;
        if current_cell_size + added > kind.offset {
            Self::rearrange(internal);
        }

        let kind = internal.meta.kind.leaf();
        assert!(current_cell_size + added <= kind.offset, "added cell overflowed to the offset. current_cell_size={current_cell_size} added={added} offset={}", kind.offset);
        kind.offset - raw_size
    }

    pub(crate) fn insert_content(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        content: &mut impl Content,
        key_size: usize,
        value_size: usize,
        overflow: Option<PageId>,
    ) -> anyhow::Result<bool> {
        let pgid = self.id();
        let content_size = content.remaining();
        let mut internal = self.internal_mut();
        let payload_size =
            internal.buffer.len() - PAGE_HEADER_SIZE - LEAF_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - LEAF_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let kind = internal.meta.kind.leaf();
        if kind.remaining < LEAF_CELL_SIZE {
            return Ok(false);
        }
        let remaining = kind.remaining - LEAF_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < content_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, content_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let reserved_offset = Self::reserve_cell(&mut internal, raw_size);
        content.put(&mut internal.buffer[reserved_offset..reserved_offset + raw_size])?;

        internal.meta.lsn = ctx.record_leaf_insert(
            internal.txid,
            pgid,
            i,
            Bytes::new(&internal.buffer[reserved_offset..reserved_offset + raw_size]),
            overflow,
            key_size,
            value_size,
        )?;
        internal.meta.dirty = true;

        Self::insert_cell_meta(&mut internal, i, overflow, key_size, value_size, raw_size);
        Ok(true)
    }

    fn insert_cell_meta(
        internal: &mut PageInternalWrite,
        index: usize,
        overflow: Option<PageId>,
        key_size: usize,
        val_size: usize,
        raw_size: usize,
    ) {
        let pgid = internal.meta.id();
        let kind = internal.meta.kind.interior_mut();
        let added = LEAF_CELL_SIZE + raw_size;
        assert!(
            index <= kind.count,
            "insert cell meta on pgid={pgid:?} index out of bound. index={index}, count={}",
            kind.count,
        );
        let current_cell_size =
            PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * kind.count;
        assert!(current_cell_size + added <= kind.offset);

        let shifted = kind.count - index;
        for i in 0..shifted {
            let cell_offset =
                PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * (kind.count - i);
            let (a, b) = internal.buffer.split_at_mut(cell_offset);
            b[..LEAF_CELL_SIZE].copy_from_slice(&a[a.len() - LEAF_CELL_SIZE..]);
        }

        let cell = &mut internal.buffer.payload_mut()[get_leaf_cell_range(index)];
        kind.offset -= raw_size;
        kind.remaining -= added;
        kind.count += 1;
        cell[LEAF_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow.to_be_bytes());
        cell[LEAF_CELL_KEY_SIZE_RANGE].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[LEAF_CELL_VAL_SIZE_RANGE].copy_from_slice(&(val_size as u32).to_be_bytes());
        cell[LEAF_CELL_OFFSET_RANGE].copy_from_slice(&(kind.offset as u16).to_be_bytes());
        cell[LEAF_CELL_SIZE_RANGE].copy_from_slice(&(raw_size as u16).to_be_bytes());
    }

    fn rearrange(internal: &mut PageInternalWrite) {
        use super::MAXIMUM_PAGE_SIZE;
        use std::cell::RefCell;
        std::thread_local! {
            static TEMP_BUFFER: RefCell<[u8; MAXIMUM_PAGE_SIZE]> = RefCell::new([0u8; MAXIMUM_PAGE_SIZE]);
        }

        let kind = internal.meta.kind.leaf_mut();
        TEMP_BUFFER.with_borrow_mut(|copied| {
            let count = kind.count;
            let page_size = internal.buffer.len();
            copied.copy_from_slice(internal.buffer);

            let mut new_offset = page_size - PAGE_FOOTER_SIZE;
            for i in 0..count {
                let copied_cell = get_leaf_cell(copied.payload(), i);
                let copied_content = copied_cell.raw();
                new_offset -= copied_content.len();
                internal.buffer[new_offset..new_offset + copied_content.len()]
                    .copy_from_slice(copied_content);

                let payload = internal.buffer.payload_mut();
                let cell = &mut payload[get_leaf_cell_range(i)];
                cell[LEAF_CELL_OFFSET_RANGE].copy_from_slice(&(new_offset as u16).to_be_bytes());
            }
            kind.offset = new_offset;
        })
    }

    pub(crate) fn split<F>(&mut self, ctx: LogContext<'_>, f: F) -> anyhow::Result<usize>
    where
        for<'c> F: Fn(LeafCell<'c>) -> anyhow::Result<()>,
    {
        let pgid = self.id();
        let internal = self.internal();
        log::debug!(
            "leaf_split {pgid:?} kind={:?} page_lsn={:?}",
            internal.meta.kind,
            internal.meta.lsn,
        );
        let page_size = internal.buffer.len();

        let payload_size = page_size - PAGE_HEADER_SIZE - LEAF_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let half_payload = payload_size / 2;

        let mut cummulative_size = 0;
        let mut n_cells_to_keep = 0;

        let kind = internal.meta.kind.leaf();
        for i in 0..kind.count {
            let cell = self.get(i);
            let new_cummulative_size = cummulative_size + cell.raw.len() + LEAF_CELL_SIZE;
            if new_cummulative_size >= half_payload {
                n_cells_to_keep = i;
                break;
            }
            cummulative_size = new_cummulative_size;
        }
        assert!(
            n_cells_to_keep < kind.count,
            "there is no point splitting the page if it doesn't move any entries. n_cells_to_keep={n_cells_to_keep} count={}", kind.count,
        );

        let internal = self.internal_mut();
        let kind = internal.meta.kind.leaf_mut();
        let txid = internal.txid;
        for i in (n_cells_to_keep..kind.count).rev() {
            let cell = get_leaf_cell(internal.buffer.payload(), i);
            internal.meta.lsn = ctx.record_leaf_delete(
                txid,
                pgid,
                i,
                Bytes::new(cell.raw()),
                cell.overflow(),
                cell.key_size(),
                cell.val_size(),
            )?;
        }
        internal.meta.dirty = true;

        let original_count = kind.count;
        kind.count = n_cells_to_keep;
        kind.remaining = payload_size - cummulative_size;

        log::debug!(
            "leaf_split_finish {pgid:?} kind={:?} page_lsn={:?}",
            internal.meta.kind,
            internal.meta.lsn,
        );

        for i in n_cells_to_keep..original_count {
            let cell = self.get(i);
            f(cell)?;
        }
        Ok(n_cells_to_keep)
    }
}

pub(crate) struct OverflowPageRead<T>(T);

pub(crate) trait OverflowPage<'a>: PageOps<'a> {
    #[inline]
    fn next(&self) -> Option<PageId> {
        self.internal().meta.kind.overflow().next
    }

    #[inline]
    fn content(&self) -> &[u8] {
        let internal = self.internal();
        let size = internal.meta.kind.overflow().size;
        let offset = PAGE_HEADER_SIZE + OVERFLOW_PAGE_HEADER_SIZE;
        &internal.buffer[offset..offset + size]
    }
}

impl<'a, T> PageOps<'a> for OverflowPageRead<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> OverflowPage<'a> for OverflowPageRead<T> where T: PageOps<'a> {}

pub(crate) struct OverflowPageWrite<T>(T);

impl<'a, T> PageOps<'a> for OverflowPageWrite<T>
where
    T: PageOps<'a>,
{
    #[inline]
    fn internal(&self) -> PageInternal {
        self.0.internal()
    }
}

impl<'a, T> PageWriteOps<'a> for OverflowPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    #[inline]
    fn internal_mut(&mut self) -> PageInternalWrite {
        self.0.internal_mut()
    }
}

impl<'a, T> OverflowPage<'a> for OverflowPageWrite<T> where T: PageWriteOps<'a> {}

impl<'a, T> OverflowPageWrite<T>
where
    T: PageWriteOps<'a>,
{
    pub(crate) fn set_next(
        &mut self,
        ctx: LogContext,
        new_next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let kind = internal.meta.kind.overflow_mut();
        let old_next = kind.next;
        if old_next == new_next {
            return Ok(());
        }
        internal.meta.lsn =
            ctx.record_overflow_set_next(internal.txid, pgid, new_next, old_next)?;
        internal.meta.dirty = true;
        kind.next = new_next;
        Ok(())
    }

    pub(crate) fn set_content(
        &mut self,
        ctx: LogContext<'_>,
        content: &mut impl Content,
        next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let max_size =
            internal.buffer.len() - PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE - OVERFLOW_PAGE_HEADER_SIZE;
        let kind = internal.meta.kind.overflow_mut();
        if kind.size != 0 || kind.next.is_some() {
            return Err(anyhow!("overflow page is already filled"));
        }

        let inserted_size = std::cmp::min(content.remaining(), max_size);

        let offset = PAGE_HEADER_SIZE + OVERFLOW_PAGE_HEADER_SIZE;
        let raw = &mut internal.buffer[offset..offset + inserted_size];
        content.put(raw)?;
        internal.meta.lsn =
            ctx.record_overflow_set_content(internal.txid, pgid, Bytes::new(raw), next)?;
        internal.meta.dirty = true;

        kind.size = inserted_size;
        kind.next = next;
        Ok(())
    }

    pub(crate) fn unset_content(&mut self, ctx: LogContext<'_>) -> anyhow::Result<()> {
        let pgid = self.id();
        let internal = self.internal_mut();
        let kind = internal.meta.kind.overflow_mut();
        if kind.size == 0 {
            return Ok(());
        }

        internal.meta.lsn =
            ctx.record_overflow_set_content(internal.txid, pgid, Bytes::new(&[]), None)?;
        internal.meta.dirty = true;

        kind.size = 0;
        Ok(())
    }

    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<T> {
        let pgid = self.id();
        let internal = self.internal_mut();
        internal.meta.encode(internal.buffer)?;

        let payload = Bytes::new(internal.buffer.payload());
        internal.meta.lsn = ctx.record_overflow_reset(internal.txid, pgid, payload)?;
        internal.meta.dirty = true;
        internal.meta.kind = PageKind::None;
        Ok(self.0)
    }
}

trait PagePayload<'a> {
    fn payload(self) -> &'a [u8];
}

trait PagePayloadMut<'a> {
    fn payload_mut(self) -> &'a mut [u8];
}

impl<'a> PagePayload<'a> for &'a [u8] {
    fn payload(self) -> &'a [u8] {
        &self[PAGE_HEADER_SIZE..self.len() - PAGE_FOOTER_SIZE]
    }
}

impl<'a> PagePayload<'a> for &'a mut [u8] {
    fn payload(self) -> &'a [u8] {
        &self[PAGE_HEADER_SIZE..self.len() - PAGE_FOOTER_SIZE]
    }
}

impl<'a> PagePayloadMut<'a> for &'a mut [u8] {
    fn payload_mut(self) -> &'a mut [u8] {
        let len = self.len();
        &mut self[PAGE_HEADER_SIZE..len - PAGE_FOOTER_SIZE]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager_v2::MAXIMUM_PAGE_SIZE;

    #[test]
    fn test_encode_decode() -> anyhow::Result<()> {
        let testcases = vec![
            PageMeta {
                id: PageId::new(1).unwrap(),
                kind: PageKind::None,
                lsn: Lsn::new(1),
                dirty: false,
            },
            // TODO: test interior and leaf kind
            PageMeta {
                id: PageId::new(112314).unwrap(),
                kind: PageKind::Overflow(OverflowKind {
                    next: None,
                    size: 100,
                }),
                lsn: Lsn::new(99),
                dirty: false,
            },
            PageMeta {
                id: PageId::new(112314).unwrap(),
                kind: PageKind::Overflow(OverflowKind {
                    next: PageId::new(33),
                    size: 100,
                }),
                lsn: Lsn::new(99),
                dirty: false,
            },
            PageMeta {
                id: PageId::new(112314).unwrap(),
                kind: PageKind::Freelist(FreelistKind {
                    next: None,
                    count: 10,
                }),
                lsn: Lsn::new(99),
                dirty: false,
            },
            PageMeta {
                id: PageId::new(112314).unwrap(),
                kind: PageKind::Freelist(FreelistKind {
                    next: PageId::new(33),
                    count: 10,
                }),
                lsn: Lsn::new(99),
                dirty: false,
            },
        ];

        let page_sizes = [256usize, 512, 4096, 8192, 16384];
        let mut global_buff = vec![0u8; MAXIMUM_PAGE_SIZE];
        for testcase in testcases {
            for page_size in page_sizes.iter().copied() {
                let buff = &mut global_buff[..page_size];
                testcase.encode(buff)?;
                let Some(decoded_result) = PageMeta::decode(buff)? else {
                    panic!("encoding and decoding again should success");
                };
                assert_eq!(testcase, decoded_result);

                buff[page_size - 8..].fill(0);
                let decoded_broken_result = PageMeta::decode(buff)?;
                assert!(decoded_broken_result.is_none());
            }
        }

        Ok(())
    }
}
