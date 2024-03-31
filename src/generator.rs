//! Utilities for generating Snowflake IDs.

use std::{
    sync::atomic::{AtomicU16, AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use crossbeam_utils::CachePadded;

use crate::Snowflake;

/// A generator for Snowflake IDs.
///
/// This generator is thread-safe and can be used to generate
/// Snowflake IDs in a concurrent environment.
pub struct Generator {
    epoch: u64,
    id: u16,
    incr: CachePadded<AtomicU16>,
    time: CachePadded<AtomicU64>,
}

impl Generator {
    /// Creates a new Snowflake generator with the given epoch and ID.
    ///
    /// The epoch is the time in milliseconds since the Unix epoch (1970-01-01T00:00:00Z).
    /// This should _always_ be in the past.
    ///
    ///
    /// The ID is a unique 10-bit value for the system that the generator is running on.
    ///
    /// # Panics
    ///
    /// Panics if the ID is larger than 10 bits.
    pub const fn new(epoch: u64, id: u16) -> Self {
        assert!(id <= 0x3FF, "Snowflake ID is larger than 10 bits");

        Generator {
            epoch,
            id,
            incr: CachePadded::new(AtomicU16::new(0)),

            // NOTE: Setting this to the epoch will ensure that the epoch being ahead
            // of the current time will not cause undefined behavior. Though it will
            // be a bit annoying to have to wait for the time to catch up. Should never
            // happen in practice, though.
            time: CachePadded::new(AtomicU64::new(epoch)),
        }
    }

    /// Sets the ID for the generator.
    ///
    /// # Panics
    ///
    /// Panics if the ID is larger than 10 bits.
    pub fn set_id(&mut self, id: u16) {
        assert!(id <= 0x3FF, "Snowflake ID is larger than 10 bits");

        self.id = id;
    }

    /// Gets the ID for the generator.
    #[inline(always)]
    pub const fn id(&self) -> u16 {
        self.id
    }

    /// Gets the epoch for the generator.
    #[inline(always)]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Gets the epoch of the generator as a `SystemTime`.
    pub fn epoch_systemtime(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.epoch)
    }

    /// Given a Snowflake, resolves the `SystemTime` that it was generated at.
    pub fn resolve_systemtime(&self, sf: Snowflake) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(sf.raw_timestamp() + self.epoch)
    }

    /// Gets the epoch of the generator as a `timestamp::Timestamp`.
    #[cfg(feature = "timestamp")]
    pub fn epoch_timestamp(&self) -> timestamp::Timestamp {
        timestamp::Timestamp::UNIX_EPOCH + Duration::from_millis(self.epoch)
    }

    /// Given a Snowflake, resolves the `timestamp::Timestamp` that it was generated at.
    #[cfg(feature = "timestamp")]
    pub fn resolve_timestamp(&self, sf: Snowflake) -> timestamp::Timestamp {
        timestamp::Timestamp::UNIX_EPOCH + Duration::from_millis(sf.raw_timestamp() + self.epoch)
    }

    /// Generates a new Snowflake at the current time.
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

        // SAFETY: `ms` is always non-zero, and `self.epoch` is always before now.
        unsafe { Snowflake::from_parts(max_ms - self.epoch, self.id as u64, incr as u64) }
    }

    /// See [`Snowflake::from_unix_ms`], uses the generator's epoch.
    pub fn from_unix_ms(&self, ms: u64) -> Option<Snowflake> {
        Snowflake::from_unix_ms(self.epoch, ms)
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
