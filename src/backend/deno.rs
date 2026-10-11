//! Narrow `deno_core` seam for sibling backend crates.

use std::rc::Rc;
use std::sync::Arc;

use deno_core::{Extension, ModuleLoader};

use crate::engine::ClavironModuleLoader;
use crate::{ExtensionContext, extensions};

/// Public ESM facade contributed by a Claviron JavaScript extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtensionModule {
    pub specifier: &'static str,
    pub source: &'static str,
}

impl ExtensionModule {
    pub const fn new(
        specifier: &'static str,
        source: &'static str,
    ) -> Self {
        Self { specifier, source }
    }
}

/// Creates one fresh Deno extension for each JavaScript isolate.
///
/// Implementations may retain shared Rust services such as database pools in
/// the factory and clone those handles into each returned extension.
pub trait ExtensionFactory: Send + Sync {
    fn extension(
        &self,
        context: ExtensionContext,
    ) -> Extension;

    fn modules(&self) -> &'static [ExtensionModule] {
        &[]
    }
}

pub type SharedExtensionFactory = Arc<dyn ExtensionFactory>;

/// Builds only Claviron-owned extensions.
///
/// A full Deno worker must add these to its existing extension set and must not
/// register Deno's built-in Web/Node extensions a second time.
pub fn extensions(context: ExtensionContext) -> Vec<Extension> {
    vec![extensions::extension(context)]
}

/// Wraps a backend loader with support for the public `claviron:*` namespace.
pub fn module_loader(inner: Rc<dyn ModuleLoader>) -> Rc<dyn ModuleLoader> {
    Rc::new(ClavironModuleLoader::new(inner))
}
