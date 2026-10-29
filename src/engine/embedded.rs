use deno_core::{Extension, ExtensionFileSource, ExtensionFileSourceCode};

include!(concat!(env!("OUT_DIR"), "/extension_sources.rs"));

/// Bind bundled sources once at isolate creation, including lazy modules.
pub(crate) fn extension(mut extension: Extension) -> Extension {
    for files in [
        &mut extension.js_files,
        &mut extension.esm_files,
        &mut extension.lazy_loaded_js_files,
        &mut extension.lazy_loaded_esm_files,
    ] {
        if !files.iter().any(|file| {
            matches!(
                file.code,
                ExtensionFileSourceCode::LoadedFromFsDuringSnapshot(_)
            )
        }) {
            continue;
        }
        for file in files.to_mut() {
            if let ExtensionFileSourceCode::LoadedFromFsDuringSnapshot(path) =
                &file.code
            {
                *file = ExtensionFileSource::new(file.specifier, source(path));
            }
        }
    }
    extension
}

#[cfg(test)]
#[path = "../../tests/engine/embedded.rs"]
mod tests;
