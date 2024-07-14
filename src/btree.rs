use std::fs::remove_dir_all;

use crate::content::{Bytes, Content};
use crate::pager::{
    BTreeCell, BTreePage, InteriorCell, InteriorPageWrite, LeafPageWrite, OverflowPageRead, PageId,
    Pager,
};
use crate::wal::TxId;
use anyhow::anyhow;

pub(crate) struct BTree<'a> {
    txid: TxId,
    pager: &'a Pager,
    root: PageId,
    freelist: Option<PageId>,
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
    pub(crate) fn new(
        txid: TxId,
        pager: &'a Pager,
        root: PageId,
        freelist: Option<PageId>,
    ) -> BTree<'a> {
        BTree {
            txid,
            pager,
            root,
            freelist,
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
        // TODO: try to acquire only the leaf node and insert there
        Ok(false)
    }

    fn put_slow(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let mut result = self.lookup_for_insert(key)?;
        todo!();
    }

    fn lookup_for_insert(&self, key: &[u8]) -> anyhow::Result<LookupForUpdateResult<'a>> {
        let mut hops = Vec::default();

        let mut current = self.pager.write(self.txid, self.root)?;
        let page = loop {
            if !current.is_interior() {
                break current;
            }
            let node = current.into_interior().unwrap();

            let (i, found) = self.search_key_in_node(&node, key)?;
            let next_pgid = if i == node.count() {
                node.last()
            } else {
                node.get(i).ptr()
            };
            let next = self
                .pager
                .write(self.txid, next_pgid)
                .expect("missing page");

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

        let Some(node) = page.init_leaf()? else {
            return Err(anyhow!(
                "invalid state, btree contain non-interior and non-leaf page"
            ));
        };

        let (i, found) = self.search_key_in_node(&node, key)?;
        Ok(LookupForUpdateResult {
            interiors: hops,
            leaf: LookupHop {
                node,
                index: i,
                found,
            },
        })
    }

    fn search_key_in_node<'b>(
        &self,
        node: &'b impl BTreePage<'b>,
        key: &[u8],
    ) -> anyhow::Result<(usize, bool)> {
        // TODO: use binary search instead
        let mut i = 0;
        let mut found = false;

        while i < node.count() {
            let cell = node.get(i);
            let mut a = Bytes::new(key);
            let mut b = BtreeContent::new(self.txid, self.pager, &cell);
            let ord = a.compare(b)?;
            found = ord.is_eq();
            if ord.is_lt() {
                break;
            }
            i += 1;
        }

        Ok((i, found))
    }
}

struct BtreeContent<'a> {
    txid: TxId,
    pager: &'a Pager,
    raw: &'a [u8],
    overflow: Option<PageId>,
    remaining: usize,

    overflow_page: Option<OverflowPageRead<'a>>,
    offset: usize,
}

impl<'a> BtreeContent<'a> {
    fn new(txid: TxId, pager: &'a Pager, cell: &'a impl BTreeCell) -> Self {
        BtreeContent {
            txid,
            pager,
            raw: cell.raw(),
            overflow: cell.overflow(),
            remaining: cell.key_size(),

            overflow_page: None,
            offset: 0,
        }
    }
}

impl<'a> Content for BtreeContent<'a> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn put(&mut self, target: &mut [u8]) -> anyhow::Result<()> {
        while !target.is_empty() && !self.is_finished() {
            if let Some(ref overflow_page) = self.overflow_page {
                let data = overflow_page.content();
                if self.offset >= data.len() {
                    return Err(anyhow!("the content is not finished but raw is empty"));
                }

                let s = std::cmp::min(data.len() - self.offset, target.len());
                let s = std::cmp::min(s, self.remaining);

                target.copy_from_slice(&data[self.offset..self.offset + s]);
                self.remaining -= s;
                self.offset += s;
                if self.offset < data.len() {
                    return Ok(());
                }

                if let Some(pgid) = overflow_page.next() {
                    let Some(page) = self.pager.read(self.txid, pgid)?.into_overflow() else {
                        return Err(anyhow!("expected overflow page"));
                    };
                    self.overflow_page = Some(page);
                } else {
                    assert!(self.remaining == 0);
                }
            } else {
                if self.raw.is_empty() {
                    return Err(anyhow!("the content is not finished but raw is empty"));
                }

                let s = std::cmp::min(self.raw.len(), target.len());
                let s = std::cmp::min(s, self.remaining);
                target[..s].copy_from_slice(&self.raw[..s]);
                self.remaining -= s;
                self.raw = &self.raw[s..];
                if !self.raw.is_empty() {
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}
