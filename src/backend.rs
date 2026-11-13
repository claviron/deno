//! Compile site directives for embedded JavaScript applications.

#[doc(hidden)]
pub mod deno;

#[cfg(feature = "backend")]
mod site;
#[cfg(feature = "backend")]
pub use site::{JsBackendFactory, register};
