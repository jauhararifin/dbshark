pub(crate) trait SliceExt {
    fn read_u16(&self) -> u16;
    fn read_u32(&self) -> u32;
    fn read_u64(&self) -> u64;
}

impl SliceExt for [u8] {
    fn read_u16(&self) -> u16 {
        u16::from_be_bytes(self[..2].try_into().unwrap())
    }
    fn read_u32(&self) -> u32 {
        u32::from_be_bytes(self[..4].try_into().unwrap())
    }
    fn read_u64(&self) -> u64 {
        u64::from_be_bytes(self[..8].try_into().unwrap())
    }
}
