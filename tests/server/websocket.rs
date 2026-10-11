use super::close_details;

#[test]
fn decodes_peer_close_code_and_reason() {
    let mut payload = 1000_u16.to_be_bytes().to_vec();
    payload.extend_from_slice(b"done");

    assert_eq!(close_details(&payload), (1000, "done".to_owned(), true));
    assert_eq!(close_details(&[]), (1005, String::new(), true));
}
