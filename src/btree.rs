use crate::content::{Bytes, Content};
use crate::pager::{
    BTreeCell, BTreePage, InteriorPageWrite, LeafCell, LeafPageRead, LeafPageWrite, LogContext,
    OverflowPageRead, PageId, PageWrite, Pager,
};
use crate::wal::{TxId, Wal};
use anyhow::anyhow;
use std::cmp::Ordering;

pub(crate) struct BTree<'a> {
    txid: TxId,
    pager: &'a Pager,
    ctx: LogContext<'a>,
    root: PageId,
}

const MAX_ENTRY_SIZE: usize = 16 * 1024 * 1024;

struct LookupForUpdateResult<'a> {
    interiors: Vec<LookupHop<InteriorPageWrite<'a>>>,
    leaf: LookupHop<LeafPageWrite<'a>>,
}

struct LookupHop<T> {
    node: T,
    index: usize,
    found: bool,
}

impl<'a> BTree<'a> {
    pub(crate) fn new(txid: TxId, pager: &'a Pager, wal: &'a Wal, root: PageId) -> BTree<'a> {
        BTree {
            txid,
            pager,
            ctx: LogContext::Runtime(wal),
            root,
        }
    }

    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        if key.len() + value.len() > MAX_ENTRY_SIZE {
            return Err(anyhow!("key-value pair is too large"));
        }

        if self.put_fast(key, value)? {
            return Ok(());
        }
        self.put_slow(key, value)
    }

    fn put_fast(&self, _key: &[u8], _value: &[u8]) -> anyhow::Result<bool> {
        // TODO: try to acquire only the leaf node and insert there.
        // Unfortunately, this might not have a lot of benefit when we only allow
        // a single write txn running since there is no contention. If we want to
        // support MVCC so that we can have 1 write txn + N read txn running
        // concurrently, we might have a benefit of doing fast path.
        Ok(false)
    }

    fn put_slow(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let mut result = self.lookup_for_insert(key)?;

        if result.leaf.found {
            self.delete_leaf_cell(&mut result.leaf.node, result.leaf.index)?;
        }

        let keyval = KeyValContent::new(key, value);
        let inserted = self.insert_content_to_leaf(
            &mut result.leaf.node,
            result.leaf.index,
            keyval,
            key.len(),
        )?;
        if inserted {
            return Ok(());
        }

        let new_right_page = self.new_page()?;
        let mut new_right_leaf = new_right_page
            .init_leaf(self.ctx)?
            .expect("new page should always be convertible to leaf page");
        let new_right_pgid = new_right_leaf.id();
        let mut pivot = self.leaf_split_and_insert(
            &mut result.leaf.node,
            &mut new_right_leaf,
            result.leaf.index,
            key,
            value,
        )?;

        let is_root_leaf = result.leaf.node.id() == self.root;
        if is_root_leaf {
            let new_left_leaf = self
                .new_page()?
                .init_leaf(self.ctx)?
                .expect("new page should always be convertible to leaf page");
            self.split_root_leaf(result.leaf.node, new_left_leaf, new_right_pgid, &mut pivot)?;
        } else {
            self.propagate_interior_splitting(result.interiors, new_right_pgid, pivot)?;
        }

        Ok(())
    }

    fn lookup_for_insert(&self, key: &[u8]) -> anyhow::Result<LookupForUpdateResult<'a>> {
        let mut hops = Vec::default();

        let mut current = self.pager.write(self.txid, self.root)?;
        let page = loop {
            if !current.is_interior() {
                break current;
            }
            let node = current.into_interior().unwrap();

            let (i, found) = self.search_key_in_interior(&node, key)?;
            let next_pgid = if i == node.count() {
                node.last()
            } else {
                node.get(i).ptr()
            };
            let next = self.pager.write(self.txid, next_pgid)?;

            if !node.might_split() {
                hops.clear();
            }

            hops.push(LookupHop {
                node,
                index: i,
                found,
            });
            current = next;
        };

        let Some(node) = page.init_leaf(self.ctx)? else {
            return Err(anyhow!(
                "invalid state, btree contain non-interior and non-leaf page"
            ));
        };

        let (i, found) = self.search_key_in_leaf(&node, key)?;
        Ok(LookupForUpdateResult {
            interiors: hops,
            leaf: LookupHop {
                node,
                index: i,
                found,
            },
        })
    }

    fn search_key_in_interior(
        &self,
        node: &'a impl BTreePage<'a>,
        key: &[u8],
    ) -> anyhow::Result<(usize, bool)> {
        // TODO: use binary search instead
        let mut i = 0;
        let mut found = false;

        while i < node.count() {
            let cell = node.get(i);
            let mut a = Bytes::new(key);
            let mut b = BTreeContent::from_cell(self.pager, cell);
            let ord = a.compare(b)?;
            found = ord.is_eq();
            if ord.is_lt() {
                break;
            }
            i += 1;
        }

        Ok((i, found))
    }

    fn search_key_in_leaf(
        &self,
        node: &'a impl BTreePage<'a>,
        key: &[u8],
    ) -> anyhow::Result<(usize, bool)> {
        // TODO: use binary search instead
        let mut i = 0;
        let mut found = false;

        while i < node.count() {
            let cell = node.get(i);
            let mut a = Bytes::new(key);
            let mut b = BTreeContent::from_cell(self.pager, cell);
            let ord = a.compare(b)?;
            found = ord.is_eq();
            if ord.is_le() {
                break;
            }
            i += 1;
        }

        Ok((i, found))
    }

    fn delete_leaf_cell(&self, page: &mut LeafPageWrite, index: usize) -> anyhow::Result<()> {
        let cell = page.get(index);
        let mut overflow_pgid = cell.overflow();
        while let Some(pgid) = overflow_pgid {
            let Some(overflow) = self.pager.read(pgid)?.into_overflow() else {
                return Err(anyhow!("expected an overflow page"));
            };
            overflow_pgid = overflow.next();
            drop(overflow);
            self.delete_page(pgid)?;
        }

        page.delete(self.ctx, index)?;
        Ok(())
    }

    fn insert_content_to_leaf(
        &self,
        node: &mut LeafPageWrite,
        index: usize,
        mut content: impl Content,
        key_size: usize,
    ) -> anyhow::Result<bool> {
        let value_size = content.remaining() - key_size;
        let ok = node.insert_content(self.ctx, index, &mut content, key_size, value_size, None)?;
        if !ok {
            return Ok(false);
        }

        if content.is_finished() {
            return Ok(true);
        }

        let next_page = self.new_page()?;
        let mut overflow = next_page
            .init_overflow(self.ctx)?
            .expect("new page should always be convertible to overflow page");
        let next_pgid = overflow.id();
        node.set_cell_overflow(self.ctx, index, Some(next_pgid))?;
        overflow.set_content(self.ctx, &mut content, None)?;

        while !content.is_finished() {
            let next_page_2 = self.new_page()?;
            let mut overflow_2 = next_page_2
                .init_overflow(self.ctx)?
                .expect("new page should always be convertible to overflow page");
            let next_pgid_2 = overflow_2.id();
            overflow.set_next(self.ctx, Some(next_pgid_2))?;
            overflow_2.set_content(self.ctx, &mut content, None)?;
            overflow = overflow_2;
        }

        Ok(true)
    }

    // TODO: maybe can combine this to insert_content_to_leaf.
    fn insert_content_to_interior(
        &self,
        node: &mut InteriorPageWrite,
        index: usize,
        mut content: &mut impl Content,
        ptr: PageId,
        key_size: usize,
    ) -> anyhow::Result<bool> {
        let value_size = content.remaining() - key_size;
        let ok = node.insert_content(self.ctx, index, content, key_size, ptr, None)?;
        if !ok {
            return Ok(false);
        }

        if content.is_finished() {
            return Ok(true);
        }

        let next_page = self.new_page()?;
        let mut overflow = next_page
            .init_overflow(self.ctx)?
            .expect("new page should always be convertible to overflow page");
        let next_pgid = overflow.id();
        node.set_cell_overflow(self.ctx, index, Some(next_pgid))?;
        overflow.set_content(self.ctx, content, None)?;

        while !content.is_finished() {
            let next_page_2 = self.new_page()?;
            let mut overflow_2 = next_page_2
                .init_overflow(self.ctx)?
                .expect("new page should always be convertible to overflow page");
            let next_pgid_2 = overflow_2.id();
            overflow.set_next(self.ctx, Some(next_pgid_2))?;
            overflow_2.set_content(self.ctx, content, None)?;
            overflow = overflow_2;
        }

        Ok(true)
    }

    fn leaf_split_and_insert<'b>(
        &self,
        left_leaf: &mut LeafPageWrite,
        new_right_leaf: &'b mut LeafPageWrite,
        index: usize,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<BTreeContent<'b>>
    where
        'a: 'b,
    {
        let split = left_leaf.split(self.ctx)?;
        let n_cells_to_keep = split.n;
        for (i, cell) in split.enumerate() {
            new_right_leaf.insert_cell(self.ctx, i, cell)?;
        }
        new_right_leaf.set_next(self.ctx, left_leaf.next())?;
        left_leaf.set_next(self.ctx, Some(new_right_leaf.id()));

        let mut keyval = KeyValContent::new(key, value);
        if index < n_cells_to_keep {
            self.insert_content_to_leaf(left_leaf, index, keyval, key.len())?;
        } else {
            self.insert_content_to_leaf(
                new_right_leaf,
                index - n_cells_to_keep,
                keyval,
                key.len(),
            )?;
        };

        let pivot_cell = new_right_leaf.get(0);
        Ok(BTreeContent::from_cell(self.pager, pivot_cell))
    }

    // initial state, A is the root and a leaf node, and the only node.
    //  [A]
    //
    // if we insert a new item to A, and A split:
    // phase 1:
    //  [A]->[C]  - The first half of A's items are moved to to C.
    //            - The new item is inserted to either A or C.
    // phase 2:
    //    [A]     - A is still a root node, but now it's an interior node, its items are moved to B
    //    / \
    //   v   v    - The first half of A's initial items will be moved to B
    //  [B]->[C]  - The other half of A's initial items will be moved to C
    fn split_root_leaf(
        &self,
        a: LeafPageWrite,
        mut b: LeafPageWrite,
        c: PageId,
        mut pivot: &mut impl Content,
    ) -> anyhow::Result<()> {
        for i in 0..a.count() {
            let cell = a.get(i);
            b.insert_cell(self.ctx, i, cell)?;
        }
        let a = a.reset(self.ctx)?;
        b.set_next(self.ctx, Some(c))?;

        let mut a = a
            .init_interior(self.ctx, c)?
            .expect("resetted page should always be convertible to an interior page");

        let key_size = pivot.remaining();
        self.insert_content_to_interior(&mut a, 0, pivot, b.id(), key_size)?;

        Ok(())
    }

    fn propagate_interior_splitting(
        &mut self,
        mut interiors: Vec<LookupHop<InteriorPageWrite>>,
        mut right_pgid: PageId,
        mut pivot: impl Content,
    ) -> anyhow::Result<()> {
        let Some(interior) = interiors.pop() else {
            return Ok(());
        };

        let mut page = interior.node;
        let i = interior.index;

        let ptr = if i == page.count() {
            let last = page.last();
            page.set_last(self.ctx, right_pgid)?;
            last
        } else {
            let ptr = page.get(i).ptr();
            page.set_cell_ptr(self.ctx, i, right_pgid)?;
            ptr
        };

        let key_size = pivot.remaining();
        let inserted = self.insert_content_to_interior(&mut page, i, &mut pivot, ptr, key_size)?;
        if inserted {
            return Ok(());
        }

        let new_right_page = self.new_page()?;
        let mut new_right_interior = new_right_page
            .init_interior(self.ctx, page.last())?
            .expect("new page should always be convertible to an interior page");
        right_pgid = new_right_interior.id();

        let mut pivot =
            self.interior_split_and_insert(&mut page, &mut new_right_interior, i, pivot, ptr)?;

        let is_root = page.id() == self.root;
        if is_root {
            let new_left = self.new_page()?;
            self.split_interior_root(page, new_left, right_pgid, &mut pivot)?;
        } else {
            self.propagate_interior_splitting(interiors, right_pgid, pivot)?;
        }

        Ok(())
    }

    fn interior_split_and_insert<'b>(
        &mut self,
        left_interior: &mut InteriorPageWrite,
        new_right_interior: &'b mut InteriorPageWrite,
        index: usize,
        mut pivot: impl Content,
        ptr: PageId,
    ) -> anyhow::Result<BTreeContent<'b>>
    where
        'a: 'b,
    {
        struct Pivot {
            raw: Vec<u8>,
            key_size: usize,
            overflow: Option<PageId>,
            ptr: PageId,
        }

        let mut split = left_interior.split(self.ctx)?;
        let n_cells_to_keep = split.n;

        let first_cell = split.next().expect("split should at least emit one cell");
        let next_pivot = Pivot {
            // TODO: consider remove the allocation. Reuse the buffer instead.
            raw: first_cell.raw().to_vec(),
            key_size: first_cell.key_size(),
            overflow: first_cell.overflow(),
            ptr: first_cell.ptr(),
        };
        for (i, cell) in split.enumerate() {
            new_right_interior.insert_cell(self.ctx, i, cell)?;
        }

        new_right_interior.set_last(self.ctx, left_interior.last())?;
        left_interior.set_last(self.ctx, next_pivot.ptr)?;

        // TODO: check whether it's possible that the right node is empty

        let key_size = pivot.remaining();
        match index.cmp(&n_cells_to_keep) {
            Ordering::Less | Ordering::Equal => {
                let ok = self.insert_content_to_interior(
                    left_interior,
                    index,
                    &mut pivot,
                    ptr,
                    key_size,
                )?;
                assert!(ok);
            }
            Ordering::Greater => {
                let ok = self.insert_content_to_interior(
                    new_right_interior,
                    index - n_cells_to_keep - 1,
                    &mut pivot,
                    ptr,
                    key_size,
                )?;
                assert!(ok);
            }
        };

        Ok(BTreeContent::from_pivot(
            self.pager,
            next_pivot.raw.into_boxed_slice(),
            next_pivot.overflow,
            next_pivot.key_size,
        ))
    }

    // initial state, A is the root and an interior node.
    //  [A]
    //
    // if we insert a new item to A, and A split:
    // phase 1:
    //  [A]->[C]  - The first half of A's items are moved to to C.
    //            - The new item is inserted to either A or C.
    // phase 2:
    //    [A]     - A is still a root node, its items are moved to B
    //    / \
    //   v   v    - The first half of A's initial items will be moved to B
    //  [B]->[C]  - The other half of A's initial items will be moved to C
    fn split_interior_root(
        &self,
        a: InteriorPageWrite,
        mut b: PageWrite,
        c: PageId,
        mut pivot: &mut impl Content,
    ) -> anyhow::Result<()> {
        let a_last = a.last();
        let mut b = b
            .init_interior(self.ctx, a_last)?
            .expect("a new page  should always be convertible to an interior page");
        for i in 0..a.count() {
            let cell = a.get(i);
            b.insert_cell(self.ctx, i, cell)?;
        }

        let a = a.reset(self.ctx)?;
        let mut a = a
            .init_interior(self.ctx, c)?
            .expect("resetted page should always be convertible to an interior page");

        let key_size = pivot.remaining();
        let a_id = a.id();
        let ok = self.insert_content_to_interior(&mut a, 0, pivot, b.id(), key_size)?;
        assert!(ok);

        Ok(())
    }

    pub(crate) fn seek(&self, key: &[u8]) -> anyhow::Result<LookupResult> {
        let mut current = self.pager.read(self.root)?;
        let page = loop {
            if !current.is_interior() {
                break current;
            }
            let node = current.into_interior().unwrap();

            let (i, _) = self.search_key_in_interior(&node, key)?;
            let next_pgid = if i == node.count() {
                node.last()
            } else {
                node.get(i).ptr()
            };
            let next = self.pager.read(next_pgid)?;

            current = next;
        };

        if page.is_none() {
            return Ok(LookupResult {
                cursor: Cursor {
                    pager: self.pager,
                    page: None,
                    index: 0,
                },
                found: false,
            });
        }

        let leaf = page.into_leaf().expect("not a leaf page");

        let (i, found) = self.search_key_in_leaf(&leaf, key)?;
        let is_finished = i >= leaf.count();
        Ok(LookupResult {
            cursor: Cursor {
                pager: self.pager,
                page: if is_finished { None } else { Some(leaf) },
                index: i,
            },
            found,
        })
    }

    fn new_page(&self) -> anyhow::Result<PageWrite<'a>> {
        let Some(freelist_pgid) = self.pager.freelist() else {
            let page = self.pager.alloc(self.txid)?;
            return Ok(page);
        };

        todo!("allocate new page from freelist");
    }

    fn delete_page(&self, pgid: PageId) -> anyhow::Result<()> {
        // TODO: consider batch deletion after the end of transaction so that
        // if in a single transaction we delete a page and then allocate a page,
        // we don't have to write the freelist page twice.

        // TODO: put the page into freelist
        Ok(())
    }
}

pub(crate) struct LookupResult<'a> {
    pub(crate) cursor: Cursor<'a>,
    pub(crate) found: bool,
}

pub(crate) struct Cursor<'a> {
    pager: &'a Pager,
    page: Option<LeafPageRead<'a>>,
    index: usize,
}

pub(crate) struct KVItem {
    content: Box<[u8]>,
    key_size: usize,
}

impl KVItem {
    pub(crate) fn key(&self) -> &[u8] {
        self.content[..self.key_size].as_ref()
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.content[self.key_size..].as_ref()
    }
}

impl<'a> Cursor<'a> {
    pub(crate) fn next(&mut self) -> anyhow::Result<Option<KVItem>> {
        let Some(ref leaf_page) = self.page else {
            return Ok(None);
        };

        let item = if self.index < leaf_page.count() {
            let cell = leaf_page.get(self.index);
            let total_size = cell.key_size() + cell.val_size();
            let mut raw = vec![0u8; total_size];
            let mut content = BTreeContent::from_leaf_content(self.pager, &cell);
            content.put(&mut raw)?;

            self.index += 1;
            Some(KVItem {
                content: raw.into_boxed_slice(),
                key_size: cell.key_size(),
            })
        } else {
            None
        };

        if self.index >= leaf_page.count() {
            if let Some(next_pgid) = leaf_page.next() {
                let Some(page) = self.pager.read(next_pgid)?.into_leaf() else {
                    return Err(anyhow!("expected a leaf page"));
                };
                self.page = Some(page);
                self.index = 0;
            } else {
                self.page = None;
            }
        }

        Ok(item)
    }
}

struct KeyValContent<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> KeyValContent<'a> {
    fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key, value }
    }
}

impl Content for KeyValContent<'_> {
    fn remaining(&self) -> usize {
        self.key.len() + self.value.len()
    }

    fn put(&mut self, mut target: &mut [u8]) -> anyhow::Result<()> {
        if !self.key.is_empty() {
            let s = std::cmp::min(self.key.len(), target.len());
            target[..s].copy_from_slice(&self.key[..s]);
            self.key = &self.key[s..];
            target = &mut target[s..];
        }

        if !target.is_empty() {
            let s = std::cmp::min(self.value.len(), target.len());
            target[..s].copy_from_slice(&self.value[..s]);
            self.value = &self.value[s..];
            target = &mut target[s..];
        }

        Ok(())
    }
}

struct BTreeContent<'a> {
    pager: &'a Pager,
    remaining: usize,

    kind: BTreeContentKind<'a>,
}

enum BTreeContentKind<'a> {
    None,
    Owned {
        raw: Box<[u8]>,
        offset: usize,
        overflow: Option<PageId>,
    },
    Raw {
        raw: &'a [u8],
        overflow: Option<PageId>,
    },
    Overflow {
        overflow: OverflowPageRead<'a>,
        offset: usize,
    },
}

impl<'a> BTreeContent<'a> {
    fn from_pivot(pager: &'a Pager, raw: Box<[u8]>, overflow: Option<PageId>, size: usize) -> Self {
        BTreeContent {
            pager,
            remaining: size,
            kind: BTreeContentKind::Owned {
                raw,
                offset: 0,
                overflow,
            },
        }
    }

    fn from_cell(pager: &'a Pager, cell: impl BTreeCell<'a>) -> Self {
        let raw_size = std::cmp::min(cell.raw().len(), cell.key_size());
        let raw = &cell.raw()[..raw_size];
        BTreeContent {
            pager,
            remaining: cell.key_size(),
            kind: BTreeContentKind::Raw {
                raw,
                overflow: cell.overflow(),
            },
        }
    }

    fn from_leaf_content(pager: &'a Pager, cell: &'a LeafCell) -> Self {
        let raw_size = std::cmp::min(cell.raw().len(), cell.key_size() + cell.val_size());
        let raw = &cell.raw()[..raw_size];
        BTreeContent {
            pager,
            remaining: cell.key_size() + cell.val_size(),
            kind: BTreeContentKind::Raw {
                raw: cell.raw(),
                overflow: cell.overflow(),
            },
        }
    }
}

impl<'a> Content for BTreeContent<'a> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn put(&mut self, mut target: &mut [u8]) -> anyhow::Result<()> {
        while !target.is_empty() && !self.is_finished() {
            match &mut self.kind {
                BTreeContentKind::None => {
                    assert!(self.is_finished());
                    break;
                }
                BTreeContentKind::Owned {
                    raw,
                    ref mut offset,
                    overflow,
                } => {
                    let s = std::cmp::min(raw.len(), target.len());
                    let s = std::cmp::min(s, self.remaining);
                    target[..s].copy_from_slice(&raw[*offset..*offset + s]);
                    target = &mut target[s..];
                    self.remaining -= s;
                    *offset += s;
                    if *offset < raw.len() {
                        assert!(target.is_empty());
                        return Ok(());
                    }

                    let Some(overflow_pgid) = overflow else {
                        assert!(self.is_finished());
                        break;
                    };

                    let Some(page) = self.pager.read(*overflow_pgid)?.into_overflow() else {
                        return Err(anyhow!("expected overflow page"));
                    };
                    self.kind = BTreeContentKind::Overflow {
                        overflow: page,
                        offset: 0,
                    };
                }
                BTreeContentKind::Raw {
                    ref mut raw,
                    overflow,
                } => {
                    let s = std::cmp::min(raw.len(), target.len());
                    let s = std::cmp::min(s, self.remaining);
                    target[..s].copy_from_slice(&raw[..s]);
                    target = &mut target[s..];
                    self.remaining -= s;
                    *raw = &raw[s..];
                    if !raw.is_empty() {
                        assert!(target.is_empty());
                        return Ok(());
                    }

                    let Some(overflow_pgid) = overflow else {
                        assert!(self.is_finished());
                        break;
                    };

                    let Some(page) = self.pager.read(*overflow_pgid)?.into_overflow() else {
                        return Err(anyhow!("expected overflow page"));
                    };
                    self.kind = BTreeContentKind::Overflow {
                        overflow: page,
                        offset: 0,
                    };
                }
                BTreeContentKind::Overflow {
                    ref mut overflow,
                    ref mut offset,
                } => {
                    let data = overflow.content();
                    if *offset >= data.len() {
                        return Err(anyhow!("the content is not finished but raw is empty"));
                    }

                    let s = std::cmp::min(data.len() - *offset, target.len());
                    let s = std::cmp::min(s, self.remaining);

                    target[..s].copy_from_slice(&data[*offset..*offset + s]);
                    target = &mut target[s..];
                    self.remaining -= s;
                    *offset += s;
                    if *offset < data.len() {
                        assert!(target.is_empty());
                        break;
                    }

                    if let Some(pgid) = overflow.next() {
                        let Some(page) = self.pager.read(pgid)?.into_overflow() else {
                            return Err(anyhow!("expected overflow page"));
                        };
                        *overflow = page;
                        *offset = 0;
                    } else {
                        assert!(self.is_finished());
                        self.kind = BTreeContentKind::None;
                    }
                }
            }
        }

        Ok(())
    }
}
