use claviron::cache::{CacheJson, CacheValue};

use super::{CacheTtl, CacheWireValue, parse_ttl};

#[test]
fn preserves_64_bit_cache_values() {
    let value = CacheValue::try_from(CacheWireValue::Object(vec![(
        "counter".into(),
        CacheWireValue::U64(u64::MAX.to_string()),
    )]))
    .unwrap();

    assert_eq!(
        value,
        CacheValue::Json(CacheJson::Object(vec![(
            "counter".into(),
            CacheJson::U64(u64::MAX)
        )]))
    );
}

#[test]
fn non_positive_ttl_means_forget() {
    assert!(matches!(parse_ttl(Some("0")).unwrap(), CacheTtl::Forget));
    assert!(matches!(parse_ttl(Some("-1")).unwrap(), CacheTtl::Forget));
    assert!(matches!(parse_ttl(None).unwrap(), CacheTtl::None));
}
