use std::collections::BTreeSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use deno_core::ExtensionFileSourceCode;

#[path = "src/engine/builtin.rs"]
mod builtin;

fn local_sources(
    directory: &Path,
    paths: &mut BTreeSet<String>,
) {
    for entry in fs::read_dir(directory).expect("read runtime source directory")
    {
        let entry = entry.expect("read runtime source entry");
        let path = entry.path();
        if entry.file_type().expect("read runtime source type").is_dir() {
            local_sources(&path, paths);
        } else if path.extension().is_some_and(|value| value == "js") {
            paths.insert(path.to_str().expect("UTF-8 source path").to_owned());
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src");
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let mut paths = BTreeSet::new();
    local_sources(&root.join("src"), &mut paths);

    #[cfg(feature = "fetch")]
    let extensions = builtin::extensions(Default::default());
    #[cfg(not(feature = "fetch"))]
    let extensions = builtin::extensions();
    for extension in extensions {
        for source in extension
            .js_files
            .iter()
            .chain(extension.esm_files.iter())
            .chain(extension.lazy_loaded_js_files.iter())
            .chain(extension.lazy_loaded_esm_files.iter())
        {
            if let ExtensionFileSourceCode::LoadedFromFsDuringSnapshot(path) =
                &source.code
            {
                paths.insert((*path).to_owned());
            }
        }
    }

    let mut generated = String::from(
        "fn source(path: &str) -> deno_core::FastStaticString {\n    match path {\n",
    );
    for path in paths {
        println!("cargo:rerun-if-changed={path}");
        writeln!(
            generated,
            "        {path:?} => deno_core::ascii_str_include!({path:?}),"
        )
        .unwrap();
    }
    generated.push_str(
        "        _ => panic!(\"runtime extension source was not embedded: {path}\"),\n    }\n}\n",
    );
    let output = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    fs::write(output.join("extension_sources.rs"), generated)
        .expect("write embedded runtime source table");
}
