use anyhow::anyhow;
use std::collections::HashSet;

pub(crate) struct Evictor {
    ref_count: Vec<usize>,
    free_frames: HashSet<usize>,
    free_and_clean: HashSet<usize>,
}

impl Evictor {
    pub(crate) fn new(n: usize) -> Self {
        Self {
            ref_count: vec![0; n],
            free_frames: HashSet::default(),
            free_and_clean: HashSet::default(),
        }
    }

    pub(crate) fn acquired(&mut self, frame_id: usize) {
        assert!(frame_id < self.ref_count.len());
        self.ref_count[frame_id] += 1;
        self.free_frames.remove(&frame_id);
        self.free_and_clean.remove(&frame_id);
    }

    pub(crate) fn released(&mut self, frame_id: usize, dirty: bool) {
        assert!(frame_id < self.ref_count.len());
        self.ref_count[frame_id] -= 1;
        let free = self.ref_count[frame_id] == 0;
        if free {
            self.free_frames.insert(frame_id);
        }

        if dirty {
            self.free_and_clean.remove(&frame_id);
        }

        if !dirty && free {
            self.free_and_clean.insert(frame_id);
        }
    }

    pub(crate) fn evict(&mut self) -> anyhow::Result<(usize, bool)> {
        if let Some(frame_id) = self.free_and_clean.iter().next().copied() {
            Ok((frame_id, false))
        } else if let Some(frame_id) = self.free_frames.iter().next().copied() {
            Ok((frame_id, true))
        } else {
            return Err(anyhow!("all pages are pinned"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evict_free_page() {
        let mut evictor = Evictor::new(5);
        evictor.acquired(0);
        evictor.acquired(1);
        evictor.acquired(2);
        evictor.released(2, false);
        evictor.acquired(3);
        evictor.acquired(4);
        let (frame_id, evict) = evictor.evict().unwrap();
        assert_eq!(2, frame_id);
        assert!(!evict);
        evictor.released(3, true);
        evictor.acquired(2);
        let (frame_id, evict) = evictor.evict().unwrap();
        assert_eq!(3, frame_id);
        assert!(evict);
    }

    #[test]
    fn test_eviction_full() {
        let mut evictor = Evictor::new(5);
        evictor.acquired(0);
        assert!(evictor.evict().is_err());
        evictor.acquired(1);
        assert!(evictor.evict().is_err());
        evictor.acquired(2);
        assert!(evictor.evict().is_err());
        evictor.acquired(3);
        assert!(evictor.evict().is_err());
        evictor.acquired(4);
        assert!(evictor.evict().is_err());
    }
}
