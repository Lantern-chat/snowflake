//! Snowflake identifiers as used in Lantern Chat.
//!
//! Snowflake IDs are 64-bit unsigned integers that are guaranteed to be unique
//! within a given system. They are composed of a 42-bit timestamp, a 10-bit system ID,
//! and a 12-bit incrementing value.
//!
//! The timestamp is often the number of milliseconds since an arbitrary epoch,
//! ensuring that it won't fill up for a long time.
//!
//! The system ID is a unique identifier for the system generating the Snowflake.
//!
//! # Layout
//!
//! ```text
//! |------------------------------------------|----------|------------|
//! |             42-bit Timestamp             |  10b ID  |  12b INCR  |
//! |------------------------------------------|----------|------------|
//! ```

#![deny(missing_docs, clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]
#![cfg_attr(not(feature = "std"), no_std)]

use core::{fmt, num::NonZeroU64, str::FromStr};

#[cfg(feature = "generator")]
pub mod generator;

/// Snowflake Identifier as used in Lantern Chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Snowflake(pub NonZeroU64);

impl Snowflake {
    /// Generate the simplest valid Snowflake, the value `1`.
    #[inline(always)]
    pub const fn null() -> Snowflake {
        // SAFETY: Guaranteed to be non-zero by definition.
        Snowflake(unsafe { NonZeroU64::new_unchecked(1) })
    }

    /// Returns the timestamp bits of the Snowflake.
    #[inline]
    pub const fn raw_timestamp(&self) -> u64 {
        self.to_u64() >> 22
    }

    /// Returns the ID bits of the Snowflake, which typically represents
    /// the originating system the Snowflake was generated on.
    #[inline]
    pub const fn id(&self) -> u16 {
        // 10 bits in the middle, above the incr
        (self.to_u64() >> 12) as u16 & 0x3FF
    }

    /// Returns the increment bits of the Snowflake.
    #[inline]
    pub const fn incr(&self) -> u16 {
        // lowest 12 bits
        self.to_u64() as u16 & 0xFFF
    }

    /// Returns the Snowflake as a 64-bit unsigned integer.
    #[inline(always)]
    pub const fn to_u64(self) -> u64 {
        self.0.get()
    }

    /// Returns the Snowflake as a 64-bit signed integer.
    #[inline(always)]
    pub const fn to_i64(self) -> i64 {
        self.to_u64() as i64
    }

    /// Snowflake set to the max Signed 64-bit value, what PostgreSQL uses for `bigint`
    #[inline(always)]
    pub const fn max_safe_value() -> Snowflake {
        // SAFETY: Guaranteed to be non-zero.
        Snowflake(unsafe { NonZeroU64::new_unchecked(i64::MAX as u64) })
    }
}

impl Snowflake {
    /// Assemble a Snowflake from the given timestamp, ID, and increment.
    ///
    /// # Safety
    ///
    /// Any of the given parameters must be non-zero.
    #[inline(always)]
    pub(crate) const unsafe fn from_parts(ts: u64, id: u64, incr: u64) -> Snowflake {
        let value = (ts << 22) | (id << 12) | incr;

        debug_assert!(value != 0, "Snowflake value is zero");

        Snowflake(NonZeroU64::new_unchecked(value))
    }

    /// Create a valid Snowflake from the given unix timestamp in milliseconds.
    ///
    /// This sets the ID to `0` and the increment to `1`.
    ///
    /// This can be used as a sentinel value for database queries.
    #[inline]
    pub const fn from_unix_ms(epoch: u64, ms: u64) -> Option<Snowflake> {
        let Some(ms) = ms.checked_sub(epoch) else { return None };

        // SAFETY: Increment is always 1, ensuring non-zero
        Some(unsafe { Snowflake::from_parts(ms, 0, 1) })
    }
}

impl FromStr for Snowflake {
    type Err = <NonZeroU64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NonZeroU64::from_str(s).map(Snowflake)
    }
}

impl fmt::Display for Snowflake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "itoa")]
        return f.write_str(itoa::Buffer::new().format(self.0.get()));

        #[cfg(not(feature = "itoa"))]
        return write!(f, "{}", self.to_u64());
    }
}

#[cfg(feature = "pg")]
mod pg_impl {
    use super::*;

    use std::error::Error;

    use bytes::BytesMut;
    use postgres_types::{accepts, to_sql_checked, FromSql, IsNull, ToSql, Type};

    impl<'a> FromSql<'a> for Snowflake {
        #[inline]
        fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
            <i64 as FromSql<'a>>::from_sql(ty, raw).map(|raw| Snowflake(NonZeroU64::new(raw as u64).unwrap()))
        }

        accepts!(INT8);
    }

    impl ToSql for Snowflake {
        #[inline]
        fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
        where
            Self: Sized,
        {
            self.to_i64().to_sql(ty, out)
        }

        accepts!(INT8);
        to_sql_checked!();
    }
}

#[cfg(feature = "rusqlite")]
mod rusqlite_impl {
    use super::*;

    use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

    impl FromSql for Snowflake {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value {
                ValueRef::Integer(i) if i != 0 => unsafe { Ok(Snowflake(NonZeroU64::new_unchecked(i as u64))) },
                ValueRef::Integer(_) => Err(FromSqlError::OutOfRange(0)),
                _ => Err(FromSqlError::InvalidType),
            }
        }
    }

    impl ToSql for Snowflake {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Owned(self.to_i64().into()))
        }
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use super::*;

    use core::str::FromStr;

    use serde::de::{Deserialize, Deserializer, Error, Visitor};
    use serde::ser::{Serialize, Serializer};

    impl Serialize for Snowflake {
        #[inline]
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if serializer.is_human_readable() {
                #[cfg(feature = "itoa")]
                return serializer.serialize_str(itoa::Buffer::new().format(self.0.get()));

                #[cfg(not(feature = "itoa"))]
                return serializer.serialize_str(&self.0.get().to_string());
            } else {
                self.0.serialize(serializer)
            }
        }
    }

    impl<'de> Deserialize<'de> for Snowflake {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            return deserializer.deserialize_any(SnowflakeVisitor);

            struct SnowflakeVisitor;

            impl<'de> Visitor<'de> for SnowflakeVisitor {
                type Value = Snowflake;

                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("a 64-bit integer or numeric string")
                }

                #[inline]
                fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
                    match NonZeroU64::new(v) {
                        Some(x) => Ok(Snowflake(x)),
                        None => Err(E::custom("expected a non-zero value")),
                    }
                }

                fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
                    Snowflake::from_str(v).map_err(|e| E::custom(format!("Invalid Snowflake: {e}")))
                }
            }
        }
    }
}

#[cfg(feature = "rkyv")]
pub use rkyv_impl::{ArchivedOptionSnowflake, NicheSnowflake};

#[cfg(feature = "rkyv")]
mod rkyv_impl {
    use super::*;

    use rkyv::{
        bytecheck::CheckBytes,
        with::{ArchiveWith, DeserializeWith, SerializeWith},
        Archive, Deserialize, Fallible, Serialize,
    };

    impl Archive for Snowflake {
        type Archived = Snowflake;
        type Resolver = ();

        #[inline(always)]
        unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, out: *mut Self::Archived) {
            *out = *self;
        }
    }

    impl<S: Fallible + ?Sized> Serialize<S> for Snowflake {
        #[inline(always)]
        fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
            Ok(())
        }
    }

    impl<D: Fallible + ?Sized> Deserialize<Snowflake, D> for Snowflake {
        #[inline(always)]
        fn deserialize(&self, _deserializer: &mut D) -> Result<Snowflake, D::Error> {
            Ok(*self)
        }
    }

    impl<C: ?Sized> CheckBytes<C> for Snowflake {
        type Error = <NonZeroU64 as CheckBytes<C>>::Error;

        #[inline(always)]
        unsafe fn check_bytes<'a>(value: *const Self, context: &mut C) -> Result<&'a Self, Self::Error> {
            CheckBytes::<C>::check_bytes(value as *const NonZeroU64, context).map(|_| &*value)
        }
    }

    /// Marker type for `Option<Snowflake>` to use a niche for `None`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pub struct MyStruct {
    ///     #[with(snowflake::NicheSnowflake)]
    ///     id: Option<Snowflake>, // uses a niche for None, saving 8 bytes
    /// }
    /// ````
    pub struct NicheSnowflake;

    /// Niche-optmized `Option<Snowflake>` for use with rkyv.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[repr(transparent)]
    pub struct ArchivedOptionSnowflake(u64);

    impl<C: ?Sized> CheckBytes<C> for ArchivedOptionSnowflake {
        type Error = <u64 as CheckBytes<C>>::Error;

        #[inline(always)]
        unsafe fn check_bytes<'a>(value: *const Self, context: &mut C) -> Result<&'a Self, Self::Error> {
            CheckBytes::<C>::check_bytes(value as *const u64, context).map(|_| &*value)
        }
    }

    impl fmt::Debug for ArchivedOptionSnowflake {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.get().fmt(f)
        }
    }

    impl ArchivedOptionSnowflake {
        /// Returns `true` if the option is `None`.
        #[inline(always)]
        pub const fn is_none(&self) -> bool {
            self.0 == 0
        }

        /// Returns `true` if the option is `Some`.
        #[inline(always)]
        pub const fn is_some(&self) -> bool {
            self.0 != 0
        }

        /// Returns a reference to the inner value if the option is `Some`.
        #[inline(always)]
        pub const fn as_ref(&self) -> Option<&Snowflake> {
            match self.is_some() {
                // SAFETY: The value is non-zero, so it's safe to transmute.
                true => Some(unsafe { core::mem::transmute(&self.0) }),
                false => None,
            }
        }

        /// Returns the inner value if the option is `Some`.
        #[inline(always)]
        pub const fn get(&self) -> Option<Snowflake> {
            match self.is_some() {
                // SAFETY: The value is non-zero, so it's safe to transmute.
                true => Some(unsafe { core::mem::transmute(self.0) }),
                false => None,
            }
        }
    }

    impl PartialEq<Option<Snowflake>> for ArchivedOptionSnowflake {
        fn eq(&self, other: &Option<Snowflake>) -> bool {
            self.get().eq(other)
        }
    }

    impl ArchiveWith<Option<Snowflake>> for NicheSnowflake {
        type Archived = ArchivedOptionSnowflake;
        type Resolver = ();

        #[inline]
        unsafe fn resolve_with(
            field: &Option<Snowflake>,
            _pos: usize,
            _resolver: Self::Resolver,
            out: *mut Self::Archived,
        ) {
            let (_, fo) = rkyv::out_field!(out.0);
            fo.write(match field {
                Some(sf) => sf.to_u64(),
                None => 0,
            })
        }
    }

    impl<S: Fallible + ?Sized> SerializeWith<Option<Snowflake>, S> for NicheSnowflake {
        #[inline(always)]
        fn serialize_with(_field: &Option<Snowflake>, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
            Ok(())
        }
    }

    impl<D: Fallible + ?Sized> DeserializeWith<ArchivedOptionSnowflake, Option<Snowflake>, D> for NicheSnowflake {
        #[inline(always)]
        fn deserialize_with(
            field: &ArchivedOptionSnowflake,
            _deserializer: &mut D,
        ) -> Result<Option<Snowflake>, D::Error> {
            Ok(field.get())
        }
    }
}

#[cfg(feature = "schemars")]
mod schema_impl {
    use super::Snowflake;

    use schemars::_serde_json::json;
    use schemars::{
        schema::{InstanceType, Metadata, Schema, SchemaObject, SingleOrVec},
        JsonSchema,
    };

    impl JsonSchema for Snowflake {
        fn schema_name() -> String {
            "Snowflake".to_owned()
        }

        fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> Schema {
            let mut obj = SchemaObject {
                metadata: Some(Box::new(Metadata {
                    description: Some("Snowflake (Non-Zero Integer as String)".to_owned()),
                    examples: vec![json!("354745245846793448")],
                    ..Default::default()
                })),
                instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::String))),
                ..Default::default()
            };

            // https://stackoverflow.com/a/7036407/2083075
            obj.string().pattern = Some("^[0-9]*[1-9][0-9]*$".to_owned()); // non-zero integer

            Schema::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sf_size() {
        use core::mem::size_of;
        assert_eq!(size_of::<u64>(), size_of::<Option<Snowflake>>());
    }
}
