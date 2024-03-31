#![cfg(feature = "serde")]

use snowflake::Snowflake;

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
