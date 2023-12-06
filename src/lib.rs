use std::{fmt, num::NonZeroU64, str::FromStr};

pub mod generator;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Snowflake(pub NonZeroU64);

impl Snowflake {
    /// Generate the simplest valid Snowflake
    #[inline(always)]
    pub const fn null() -> Snowflake {
        Snowflake(unsafe { NonZeroU64::new_unchecked(1) })
    }

    #[inline]
    pub const fn raw_timestamp(&self) -> u64 {
        self.to_u64() >> 22
    }

    #[inline]
    pub const fn id(&self) -> u16 {
        // 10 bits in the middle, above the incr
        (self.to_u64() >> 12) as u16 & 0x3FF
    }

    #[inline]
    pub const fn incr(&self) -> u16 {
        // lowest 12 bits
        self.to_u64() as u16 & 0xFFF
    }

    #[inline(always)]
    pub const fn to_u64(self) -> u64 {
        self.0.get()
    }

    #[inline(always)]
    pub const fn to_i64(self) -> i64 {
        unsafe { std::mem::transmute(self.to_u64()) }
    }

    /// Snowflake set to the max Signed 64-bit value, what PostgreSQL uses for `bigint`
    #[inline(always)]
    pub const fn max_safe_value() -> Snowflake {
        Snowflake(unsafe { NonZeroU64::new_unchecked(i64::MAX as u64) })
    }
}

impl Snowflake {
    #[inline(always)]
    pub(crate) const fn from_parts(ts: u64, id: u64, incr: u64) -> Snowflake {
        Snowflake(unsafe { NonZeroU64::new_unchecked((ts << 22) | (id << 12) | incr) })
    }

    /// Create a valid Snowflake from the given unix timestamp in milliseconds.
    #[inline]
    pub const fn from_unix_ms(epoch: u64, ms: u64) -> Option<Snowflake> {
        let Some(ms) = ms.checked_sub(epoch) else { return None };
        Some(Snowflake::from_parts(ms, 0, 1))
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

    use std::str::FromStr;

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
pub struct NicheSnowflake;

#[cfg(feature = "rkyv")]
const _: () = {
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

    use rkyv::niche::option_nonzero::ArchivedOptionNonZeroU64;

    impl ArchiveWith<Option<Snowflake>> for NicheSnowflake {
        type Archived = ArchivedOptionNonZeroU64;
        type Resolver = ();

        #[inline(always)]
        unsafe fn resolve_with(
            field: &Option<Snowflake>,
            _pos: usize,
            _resolver: Self::Resolver,
            out: *mut Self::Archived,
        ) {
            ArchivedOptionNonZeroU64::resolve_from_option(field.map(|sf| sf.0), out as _);
        }
    }

    impl<S: Fallible + ?Sized> SerializeWith<Option<Snowflake>, S> for NicheSnowflake {
        #[inline(always)]
        fn serialize_with(_field: &Option<Snowflake>, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
            Ok(())
        }
    }

    impl<D: Fallible + ?Sized> DeserializeWith<ArchivedOptionNonZeroU64, Option<Snowflake>, D> for NicheSnowflake {
        #[inline(always)]
        fn deserialize_with(
            field: &ArchivedOptionNonZeroU64,
            _deserializer: &mut D,
        ) -> Result<Option<Snowflake>, D::Error> {
            Ok(field.as_ref().map(|x| Snowflake(*x)))
        }
    }
};

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
        use std::mem::size_of;
        assert_eq!(size_of::<u64>(), size_of::<Option<Snowflake>>());
    }

    #[test]
    fn test_serde() {
        #[derive(Debug, Clone, Copy, serde_derive::Serialize, serde_derive::Deserialize)]
        struct Nested {
            x: Snowflake,
        }

        let _: Snowflake = serde_json::from_str(r#""12234""#).unwrap();
        let _: Snowflake = serde_json::from_str(r#"12234"#).unwrap();
        let _: Nested = serde_json::from_str(r#"{"x": 12234}"#).unwrap();
        let _: Nested = serde_json::from_str(r#"{"x": "12234"}"#).unwrap();
    }
}
