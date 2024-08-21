use crate::bins::SliceExt;
use crate::id::{Lsn, LsnExt, PageId, PageIdExt};
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

        let payload_buff = &mut buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
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
        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];

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
const fn get_interior_cell_range(index: usize) -> Range<usize> {
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
