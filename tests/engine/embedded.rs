use std::borrow::Cow;

use deno_core::{Extension, ExtensionFileSource, ExtensionFileSourceCode};

fn assert_embedded(extension: &Extension) {
    for source in extension
        .js_files
        .iter()
        .chain(extension.esm_files.iter())
        .chain(extension.lazy_loaded_js_files.iter())
        .chain(extension.lazy_loaded_esm_files.iter())
    {
        assert!(
            !matches!(
                source.code,
                ExtensionFileSourceCode::LoadedFromFsDuringSnapshot(_)
            ),
            "{} still loads from disk",
            source.specifier,
        );
        source.load().expect("embedded source remains loadable");
    }
}

#[test]
fn builtin_platform_sources_are_embedded_without_booting_an_isolate() {
    let extensions = crate::engine::platform::runtime_extensions(
        crate::ExtensionContext::new("embedded-sources"),
        &[],
    );
    assert!(!extensions.is_empty());
    for extension in extensions {
        assert_embedded(&extension);
    }
}

#[test]
fn eager_and_lazy_source_descriptors_use_embedded_bytes() {
    let source = ExtensionFileSource::loaded_during_snapshot(
        "ext:example/module.js",
        concat!(env!("CARGO_MANIFEST_DIR"), "/src/engine/platform.js"),
    );
    let extension = super::extension(Extension {
        js_files: Cow::Owned(vec![source.clone()]),
        esm_files: Cow::Owned(vec![source.clone()]),
        lazy_loaded_js_files: Cow::Owned(vec![source.clone()]),
        lazy_loaded_esm_files: Cow::Owned(vec![source]),
        ..Default::default()
    });
    assert_embedded(&extension);
}
