use super::parse_id;

#[test]
fn accepts_only_local_opaque_stream_handles() {
    assert_eq!(parse_id("stream-7").unwrap(), 7);
    assert!(parse_id("7").is_err());
    assert!(parse_id("stream-other").is_err());
}
