use anyhow::anyhow;
use std::fs::File;
use syscalls::{syscall2, Sysno};

pub(crate) trait FileLock: Sized {
    fn lock(self) -> anyhow::Result<Self>;
}

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
impl FileLock for File {
    fn lock(self) -> anyhow::Result<Self> {
        let fd = self.as_raw_fd();
        const LOCK_EX: usize = 0x2;
        let result = unsafe { syscall2(Sysno::flock, fd as usize, LOCK_EX) };
        if let Err(err) = result {
            Err(anyhow!("cannot lock file {fd} errno={err}"))
        } else {
            Ok(self)
        }
    }
}
