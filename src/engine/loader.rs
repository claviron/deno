use std::collections::BTreeMap;
use std::rc::Rc;

use deno_core::{
    ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader,
    ModuleResolveResponse, ModuleSource, ModuleSourceCode, ModuleSpecifier,
    ModuleType, ResolutionKind,
};

const RUNTIME_MODULE: &str = "claviron:runtime";
const RUNTIME_MODULE_SOURCE: &str = r#"
    const runtime = globalThis[Symbol.for("claviron.runtime")];

    export const applicationName = runtime.applicationName;
    export const environment = runtime.environment;
    export const runtimeAvailable = runtime.runtimeAvailable;
    export const isCancelled = runtime.isCancelled;
    export const cancelled = runtime.cancelled;
    export const yieldToRuntime = runtime.yieldToRuntime;
"#;

pub(crate) struct ClavironModuleLoader {
    inner: Rc<dyn ModuleLoader>,
    modules: BTreeMap<&'static str, &'static str>,
}

impl ClavironModuleLoader {
    pub(crate) fn new(inner: Rc<dyn ModuleLoader>) -> Self {
        Self::with_modules(inner, std::iter::empty())
            .expect("built-in module registry is valid")
    }

    pub(crate) fn with_modules(
        inner: Rc<dyn ModuleLoader>,
        modules: impl IntoIterator<Item = crate::backend::deno::ExtensionModule>,
    ) -> crate::Result<Self> {
        let mut registry =
            BTreeMap::from([(RUNTIME_MODULE, RUNTIME_MODULE_SOURCE)]);
        for module in modules {
            if !module.specifier.starts_with("claviron:")
                || registry.insert(module.specifier, module.source).is_some()
            {
                return Err(crate::Error::InvalidExtensionModule(
                    module.specifier,
                ));
            }
        }
        Ok(Self { inner, modules: registry })
    }
}

impl ModuleLoader for ClavironModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        kind: ResolutionKind,
    ) -> ModuleResolveResponse {
        if self.modules.contains_key(specifier) {
            return Ok(ModuleSpecifier::parse(specifier)
                .expect("validated extension module URL"));
        }

        self.inner.resolve(specifier, referrer, kind)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        maybe_referrer: Option<&ModuleLoadReferrer>,
        options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        if let Some(source) = self.modules.get(module_specifier.as_str()) {
            return ModuleLoadResponse::Sync(Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String((*source).to_owned().into()),
                module_specifier,
                None,
            )));
        }

        self.inner.load(module_specifier, maybe_referrer, options)
    }
}
