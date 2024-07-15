use std::fs::remove_dir_all;

use crate::content::{Bytes, Content};
use crate::pager::{
    BTreeCell, BTreePage, InteriorCell, InteriorPageWrite, LeafCell, LeafPageRead, LeafPageWrite,
    OverflowPageRead, PageId, Pager,
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

        if result.leaf.found {
            todo!("delete the existing cell");
        }

        let keyval = KeyValContent::new(key, value);
        if self.insert_content_to_leaf(
            &mut result.leaf.node,
            result.leaf.index,
            keyval,
            key.len(),
        )? {
            return Ok(());
        }

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
            let mut b = BTreeContent::new(self.pager, &cell);
            let ord = a.compare(b)?;
            found = ord.is_eq();
            if ord.is_le() {
                break;
            }
            i += 1;
        }

        Ok((i, found))
    }

    fn insert_content_to_leaf(
        &self,
        node: &mut LeafPageWrite,
        index: usize,
        mut content: impl Content,
        key_size: usize,
    ) -> anyhow::Result<bool> {
        let ok = node.insert_content(index, &mut content, key_size)?;
        if !ok {
            return Ok(false);
        }

        if content.is_finished() {
            return Ok(true);
        }

        todo!("insert the remaining content to overflow pages");
    }

    pub(crate) fn seek(&self, key: &[u8]) -> anyhow::Result<LookupResult> {
        let mut current = self.pager.read(self.root)?;
        let page = loop {
            if !current.is_interior() {
                break current;
            }
            let node = current.into_interior().unwrap();

            let (i, _) = self.search_key_in_node(&node, key)?;
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
        let (i, found) = self.search_key_in_node(&leaf, key)?;
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
    raw: &'a [u8],
    overflow: Option<PageId>,
    remaining: usize,

    overflow_page: Option<OverflowPageRead<'a>>,
    offset: usize,
}

impl<'a> BTreeContent<'a> {
    fn new(pager: &'a Pager, cell: &'a impl BTreeCell) -> Self {
        BTreeContent {
            pager,
            raw: cell.raw(),
            overflow: cell.overflow(),
            remaining: cell.key_size(),

            overflow_page: None,
            offset: 0,
        }
    }

    fn from_leaf_content(pager: &'a Pager, cell: &'a LeafCell) -> Self {
        BTreeContent {
            pager,
            raw: cell.raw(),
            overflow: cell.overflow(),
            remaining: cell.key_size() + cell.val_size(),

            overflow_page: None,
            offset: 0,
        }
    }
}

impl<'a> Content for BTreeContent<'a> {
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
                    let Some(page) = self.pager.read(pgid)?.into_overflow() else {
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
