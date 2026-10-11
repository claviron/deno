use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use deno_ast::{MediaType, ParseParams, SourceMapOption};
use deno_core::error::ModuleLoaderError;
use deno_core::{
    ModuleCodeString, ModuleLoadOptions, ModuleLoadReferrer,
    ModuleLoadResponse, ModuleLoader, ModuleName, ModuleSource,
    ModuleSourceCode, ModuleSpecifier, ModuleType, ResolutionKind,
    SourceMapData, resolve_import,
};
use deno_error::JsErrorBox;

type SourceMapStore = Rc<RefCell<HashMap<String, Vec<u8>>>>;

pub(crate) fn transpile_extension(
    name: ModuleName,
    source: ModuleCodeString,
) -> Result<(ModuleCodeString, Option<SourceMapData>), JsErrorBox> {
    let name = name.to_string();
    let media_type = MediaType::from_path(Path::new(&name));
    if !matches!(
        media_type,
        MediaType::TypeScript
            | MediaType::Mts
            | MediaType::Cts
            | MediaType::Dts
            | MediaType::Dmts
            | MediaType::Dcts
            | MediaType::Tsx
    ) {
        return Ok((source, None));
    }

    let specifier =
        ModuleSpecifier::parse(&name).map_err(JsErrorBox::from_err)?;
    let parsed = deno_ast::parse_module(ParseParams {
        specifier,
        text: source.into(),
        media_type,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(JsErrorBox::from_err)?;
    let transpiled = parsed
        .transpile(
            &deno_ast::TranspileOptions {
                imports_not_used_as_values:
                    deno_ast::ImportsNotUsedAsValues::Remove,
                decorators: deno_ast::DecoratorsTranspileOption::Ecma,
                ..Default::default()
            },
            &deno_ast::TranspileModuleOptions { module_kind: None },
            &deno_ast::EmitOptions {
                source_map: SourceMapOption::Separate,
                inline_sources: true,
                ..Default::default()
            },
        )
        .map_err(JsErrorBox::from_err)?
        .into_source();

    Ok((
        transpiled.text.into(),
        transpiled.source_map.map(|source_map| source_map.into_bytes().into()),
    ))
}

/// Local-file ESM loader with TypeScript/JSX transpilation and source maps.
///
/// This deliberately performs no type checking. Bare package imports resolve
/// through the nearest `node_modules` package and its ESM `exports` entry;
/// Node built-ins and CommonJS execution are not emulated.
#[derive(Default)]
pub(crate) struct TypeScriptModuleLoader {
    source_maps: SourceMapStore,
}

impl ModuleLoader for TypeScriptModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, ModuleLoaderError> {
        if is_bare_specifier(specifier) {
            return resolve_package(specifier, referrer);
        }
        resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let source_maps = Rc::clone(&self.source_maps);
        let module_specifier = module_specifier.clone();
        ModuleLoadResponse::Sync(load_module(source_maps, &module_specifier))
    }

    fn get_source_map(
        &self,
        specifier: &str,
    ) -> Option<Cow<'_, [u8]>> {
        self.source_maps.borrow().get(specifier).cloned().map(Cow::Owned)
    }
}

fn is_bare_specifier(specifier: &str) -> bool {
    !specifier.starts_with('.')
        && !specifier.starts_with('/')
        && ModuleSpecifier::parse(specifier).is_err()
}

fn resolve_package(
    specifier: &str,
    referrer: &str,
) -> Result<ModuleSpecifier, ModuleLoaderError> {
    let (package, subpath) = split_package_specifier(specifier);
    let referrer = ModuleSpecifier::parse(referrer)
        .map_err(JsErrorBox::from_err)?
        .to_file_path()
        .map_err(|_| {
            JsErrorBox::generic(
                "bare package imports require a file:// referrer",
            )
        })?;
    let directory = referrer.parent().unwrap_or(Path::new("/"));

    for ancestor in directory.ancestors() {
        let package_dir = ancestor.join("node_modules").join(package);
        let package_json = package_dir.join("package.json");
        if !package_json.is_file() {
            continue;
        }
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&package_json).map_err(JsErrorBox::from_err)?,
        )
        .map_err(JsErrorBox::from_err)?;
        let target = package_export(&manifest, subpath).ok_or_else(|| {
            JsErrorBox::generic(format!(
                "package '{package}' does not export '{}'; manifest: {}",
                if subpath.is_empty() { "." } else { subpath },
                package_json.display()
            ))
        })?;
        return file_specifier(package_dir.join(target));
    }

    Err(JsErrorBox::generic(format!(
        "package '{package}' was not found from {}",
        directory.display()
    )))
}

fn split_package_specifier(specifier: &str) -> (&str, &str) {
    if specifier.starts_with('@') {
        let mut separators = specifier.match_indices('/');
        let _ = separators.next();
        if let Some((index, _)) = separators.next() {
            (&specifier[..index], &specifier[index + 1..])
        } else {
            (specifier, "")
        }
    } else if let Some((package, subpath)) = specifier.split_once('/') {
        (package, subpath)
    } else {
        (specifier, "")
    }
}

fn package_export<'a>(
    manifest: &'a serde_json::Value,
    subpath: &str,
) -> Option<&'a str> {
    let key = if subpath.is_empty() {
        ".".to_owned()
    } else {
        format!("./{subpath}")
    };
    let export = manifest.get("exports").and_then(|exports| exports.get(&key));
    select_esm_target(export)
        .or_else(|| {
            if subpath.is_empty() {
                manifest.get("module").and_then(serde_json::Value::as_str)
            } else {
                None
            }
        })
        .or_else(|| {
            if subpath.is_empty() {
                manifest.get("main").and_then(serde_json::Value::as_str)
            } else {
                None
            }
        })
}

fn select_esm_target(value: Option<&serde_json::Value>) -> Option<&str> {
    let value = value?;
    value.as_str().or_else(|| {
        value.get("import").and_then(serde_json::Value::as_str).or_else(|| {
            value.get("default").and_then(serde_json::Value::as_str)
        })
    })
}

fn file_specifier(path: PathBuf) -> Result<ModuleSpecifier, ModuleLoaderError> {
    ModuleSpecifier::from_file_path(&path).map_err(|()| {
        JsErrorBox::generic(format!(
            "package export is not a valid file path: {}",
            path.display()
        ))
    })
}

fn load_module(
    source_maps: SourceMapStore,
    module_specifier: &ModuleSpecifier,
) -> Result<ModuleSource, ModuleLoaderError> {
    // A `claviron:` module reaching the file loader is one this build does
    // not carry: its extension's feature is off (`claviron:cache` needs
    // `cache`, `claviron:db` needs `postgres`, `claviron:orbit/*` needs
    // `orbit`). Say that, not that the specifier is not a file.
    if module_specifier.scheme() == "claviron" {
        return Err(JsErrorBox::generic(format!(
            "{module_specifier} is not available in this build: the Claviron feature that provides it is not enabled"
        )));
    }
    let path = module_specifier.to_file_path().map_err(|_| {
        JsErrorBox::generic(format!(
            "{module_specifier}: only file:// JavaScript modules are supported"
        ))
    })?;
    let media_type = MediaType::from_path(&path);
    let (module_type, transpile) = match media_type {
        MediaType::JavaScript | MediaType::Mjs | MediaType::Cjs => {
            (ModuleType::JavaScript, false)
        }
        MediaType::Jsx
        | MediaType::TypeScript
        | MediaType::Mts
        | MediaType::Cts
        | MediaType::Dts
        | MediaType::Dmts
        | MediaType::Dcts
        | MediaType::Tsx => (ModuleType::JavaScript, true),
        MediaType::Json => (ModuleType::Json, false),
        _ => {
            return Err(JsErrorBox::generic(format!(
                "unsupported JavaScript module extension: {}",
                path.display()
            )));
        }
    };

    let source =
        std::fs::read_to_string(&path).map_err(JsErrorBox::from_err)?;
    let source = if transpile {
        let parsed = deno_ast::parse_module(ParseParams {
            specifier: module_specifier.clone(),
            text: source.into(),
            media_type,
            capture_tokens: false,
            scope_analysis: false,
            maybe_syntax: None,
        })
        .map_err(JsErrorBox::from_err)?;
        let transpiled = parsed
            .transpile(
                &deno_ast::TranspileOptions {
                    imports_not_used_as_values:
                        deno_ast::ImportsNotUsedAsValues::Remove,
                    decorators: deno_ast::DecoratorsTranspileOption::Ecma,
                    ..Default::default()
                },
                &deno_ast::TranspileModuleOptions { module_kind: None },
                &deno_ast::EmitOptions {
                    source_map: SourceMapOption::Separate,
                    inline_sources: true,
                    ..Default::default()
                },
            )
            .map_err(JsErrorBox::from_err)?
            .into_source();
        if let Some(source_map) = transpiled.source_map {
            source_maps
                .borrow_mut()
                .insert(module_specifier.to_string(), source_map.into_bytes());
        }
        transpiled.text
    } else {
        source
    };

    Ok(ModuleSource::new(
        module_type,
        ModuleSourceCode::String(source.into()),
        module_specifier,
        None,
    ))
}
