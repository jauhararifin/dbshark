pub(crate) trait Content {
    fn remaining(&self) -> usize;
    fn put(&mut self, buff: &mut [u8]) -> anyhow::Result<()>;

    fn is_finished(&self) -> bool {
        self.remaining() == 0
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
