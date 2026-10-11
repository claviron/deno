use super::ExtensionContext;

#[test]
fn application_instance_is_explicit_runtime_context() {
    let context = ExtensionContext::new("api");
    assert_eq!(context.application_instance(), None);

    let context = context.with_application_instance(3);
    assert_eq!(context.application_name(), "api");
    assert_eq!(context.application_instance(), Some(3));
}
