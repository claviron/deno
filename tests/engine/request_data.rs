use super::*;

#[test]
fn native_request_buffers_are_charged_until_transferred_or_collected() {
    let mut runtime = deno_core::JsRuntime::new(Default::default());
    let parts = http::Request::builder()
        .uri("http://example.test/")
        .header("x-example", "value")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let body = vec![7; 4096];
    let capacity = body.capacity() as i64;
    let data = RequestData::new(parts, body);
    let external = data.external.clone();
    let object = {
        deno_core::scope!(scope, runtime);
        let object = data.into_v8(scope);
        assert!(external.get() > capacity);
        v8::Global::new(scope, object)
    };
    runtime.v8_isolate().low_memory_notification();
    assert!(
        external.get() > capacity,
        "a retained Request keeps its native charge"
    );
    {
        deno_core::scope!(scope, runtime);
        let object = v8::Local::new(scope, &object);
        let data = deno_core::cppgc::try_unwrap_cppgc_object::<RequestData>(
            scope,
            object.into(),
        )
        .unwrap();
        let before = external.get();
        assert_eq!(data.take_body(scope).unwrap(), vec![7; 4096]);
        assert_eq!(external.get(), before - capacity);
        assert!(data.take_body(scope).is_err());
    }
    drop(object);
    runtime.v8_isolate().low_memory_notification();
    assert_eq!(
        external.get(),
        0,
        "collection must release the remaining native charge"
    );
}
