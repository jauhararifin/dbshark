use std::cmp::Ordering;

pub(crate) trait Content {
    fn remaining(&self) -> usize;
    fn put(&mut self, buff: &mut [u8]) -> anyhow::Result<()>;

    fn is_finished(&self) -> bool {
        self.remaining() == 0
    }

    fn compare<T: Content>(&mut self, mut other: T) -> anyhow::Result<Ordering> {
        let mut buff_0 = [0u8; 1024];
        let mut buff_1 = [0u8; 1024];

        let mut sb = &buff_0[..0];
        let mut tb = &buff_1[..0];
        while (!sb.is_empty() || !self.is_finished()) && (!tb.is_empty() || !other.is_finished()) {
            if sb.is_empty() && !self.is_finished() {
                let s = std::cmp::min(self.remaining(), buff_0.len());
                self.put(&mut buff_0)?;
                sb = &buff_0[..s];
            }

            if tb.is_empty() && !other.is_finished() {
                let s = std::cmp::min(other.remaining(), buff_1.len());
                other.put(&mut buff_1)?;
                tb = &buff_1[..s];
            }

            let s = std::cmp::min(sb.len(), tb.len());
            let ord = sb[..s].cmp(&tb[..s]);
            if !ord.is_eq() {
                return Ok(ord);
            }
            sb = &sb[s..];
            tb = &tb[s..];
        }

        if self.is_finished() && sb.is_empty() && other.is_finished() && tb.is_empty() {
            Ok(Ordering::Equal)
        } else if self.is_finished() && sb.is_empty() {
            Ok(Ordering::Less)
        } else {
            Ok(Ordering::Greater)
        }
    }
}

pub(crate) struct Bytes<'a>(&'a [u8]);

impl Bytes<'_> {
    pub(crate) fn new(bytes: &[u8]) -> Bytes {
        Bytes(bytes)
    }
}

impl<'a> Content for Bytes<'a> {
    fn remaining(&self) -> usize {
        self.0.len()
    }

    fn put(&mut self, buff: &mut [u8]) -> anyhow::Result<()> {
        let s = std::cmp::min(buff.len(), self.0.len());
        buff[..s].copy_from_slice(&self.0[..s]);
        self.0 = &self.0[s..];
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes() {
        let mut a = Bytes::new(b"abc");
        let mut b = Bytes::new(b"abc");
        assert_eq!(a.compare(b).unwrap(), Ordering::Equal);

        let mut a = Bytes::new(b"abc");
        let mut b = Bytes::new(b"abd");
        assert_eq!(a.compare(b).unwrap(), Ordering::Less);

        let mut a = Bytes::new(b"abc");
        let mut b = Bytes::new(b"ab");
        assert_eq!(a.compare(b).unwrap(), Ordering::Greater);

        let mut a = Bytes::new(b"ab");
        let mut b = Bytes::new(b"abc");
        assert_eq!(a.compare(b).unwrap(), Ordering::Less);
    }
}
