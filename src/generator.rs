use std::{
    sync::atomic::{AtomicU16, AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use crossbeam_utils::CachePadded;

use crate::Snowflake;

pub struct Generator {
    epoch: u64,
    id: u16,
    incr: CachePadded<AtomicU16>,
    time: CachePadded<AtomicU64>,
}

impl Generator {
    pub const fn new(epoch: u64, id: u16) -> Self {
        assert!(id <= 0x3FF, "Snowflake ID is larger than 10 bits");

        Generator {
            epoch,
            id,
            incr: CachePadded::new(AtomicU16::new(0)),
            time: CachePadded::new(AtomicU64::new(0)),
        }
    }

    pub fn set_id(&mut self, id: u16) {
        assert!(id <= 0x3FF, "Snowflake ID is larger than 10 bits");

        self.id = id;
    }

    #[inline(always)]
    pub const fn id(&self) -> u16 {
        self.id
    }

    #[inline(always)]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn epoch_systemtime(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.epoch)
    }

    pub fn resolve_systemtime(&self, sf: Snowflake) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(sf.raw_timestamp() + self.epoch)
    }

    #[cfg(feature = "timestamp")]
    pub fn epoch_timestamp(&self) -> timestamp::Timestamp {
        timestamp::Timestamp::UNIX_EPOCH + Duration::from_millis(self.epoch)
    }

    #[cfg(feature = "timestamp")]
    pub fn resolve_timestamp(&self, sf: Snowflake) -> timestamp::Timestamp {
        timestamp::Timestamp::UNIX_EPOCH + Duration::from_millis(sf.raw_timestamp() + self.epoch)
    }

    pub fn gen(&self) -> Snowflake {
        let ms = SystemTime::UNIX_EPOCH.elapsed().expect("Could not get time").as_millis() as u64;

        // SAFETY: We always return `Some` in this, so it's impossible for `fetch_update` to fail
        let incr = unsafe {
            self.incr
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |incr| Some((incr + 1) & 0xFFF))
                .unwrap_unchecked()
        };

        // NOTE: `fetch_max` does not actually return the max, but the old value, so another max is needed.
        let mut max_ms = self.time.fetch_max(ms, Ordering::SeqCst).max(ms);

        // clock went backwards and incr is/was at max
        if incr == 0xFFF && max_ms > ms {
            // forcibly increment the timestamp by 1ms until clock flows normally again
            while let Err(new_max) =
                self.time.compare_exchange_weak(max_ms, max_ms + 1, Ordering::AcqRel, Ordering::Relaxed)
            {
                max_ms = new_max;
            }

            // TODO: Maybe add a log entry for this
        }

        Snowflake::from_parts(max_ms - self.epoch, self.id as u64, incr as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_many_snowflakes() {
        let generator = Generator::new(1550102400000, 0);
        for _ in 0..100 {
            _ = generator.gen();
        }
    }
}
