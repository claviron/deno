//! Slim embedded JavaScript applications for Claviron consumers.
//!
//! The public surface speaks in application concepts. The `deno_core` bridge
//! is isolated under the hidden [`backend`] module so a full Deno backend can
//! reuse Claviron extensions without exposing engine types to applications.

mod application;
#[cfg(feature = "server")]
pub mod config;
mod engine;
mod error;
pub mod extensions;
#[cfg(feature = "server")]
pub mod routing;
#[cfg(feature = "server")]
mod server;

#[doc(hidden)]
pub mod backend;
#[cfg(feature = "tasks")]
pub mod task;

pub use application::{
    ApplicationDefinition, ApplicationRequest, ApplicationResponse,
    CoreApplication, RequestBody,
};
pub use error::{Error, Result};
pub use extensions::ExtensionContext;
#[cfg(feature = "server")]
pub use routing::build_application_router;
#[cfg(feature = "server")]
pub use server::{
    JsApplicationAdapter, JsApplicationController, JsApplicationService,
    register,
};
#[cfg(feature = "websocket")]
pub use server::{
    JsWebSocketUpgradeHandler, WebSocketLease, header_pairs, serve_websocket,
};
