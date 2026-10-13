//! Cold compilation of the edge's `js:` directive into an owned HTTP router.

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use claviron::edge::{HostAppRegistry, HostConfig};
use claviron::http::policy::HttpPolicyOverride;
use claviron::http::routing::{RouteContext, RouteEffect, RouteExtension};
use claviron::http::{boxed_body, into_axum_response};
use claviron::runtime::AppState;
use claviron::runtime::registry::{Error, Result};

use super::JavaScriptRouteConfig;
use crate::{JsApplicationAdapter, JsApplicationService};

/// Binds a vhost route prefix to a named application prepared from `js.yaml`.
///
/// `claviron_deno::register` installs this compiler when an edge registry is
/// present. Parsing and service resolution happen only at snapshot preparation.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsRouteExtension;

impl RouteExtension<HostConfig> for JsRouteExtension {
    fn name(&self) -> &'static str {
        "js"
    }

    fn resolve(
        &self,
        context: &RouteContext<'_, HostConfig>,
        source: &serde_yaml::Value,
    ) -> Result<RouteEffect> {
        let compile = || {
            let config = parse(source)?;
            let service = context
                .state
                .registry_ref()
                .resolve::<JsApplicationService>()
                .ok_or_else(|| {
                    Error::msg("JavaScript service is not registered")
                })?;
            if !service.is_configured(&config.application) {
                return Err(Error::msg(format!(
                    "application '{}' is absent from js.yaml",
                    config.application
                )));
            }
            let adapter = Arc::new(
                JsApplicationAdapter::new(
                    Arc::clone(&service),
                    config.application.as_str(),
                )
                .route_policy(HttpPolicyOverride {
                    requests: config.requests,
                    responses: config.responses,
                }),
            );
            // Edge already selects the prefix. Preserve the admitted URI,
            // connection context and body; no second prefix dispatch is needed.
            let router = Router::new().fallback(move |request: Request| {
                let adapter = Arc::clone(&adapter);
                async move {
                    let (parts, body) = request.into_parts();
                    into_axum_response(
                        adapter
                            .handle(http::Request::from_parts(
                                parts,
                                boxed_body(body),
                            ))
                            .await,
                    )
                }
            });
            Ok(RouteEffect::Handler(with_websockets(
                router, context, &config, service,
            )?))
        };
        compile().map_err(|error| {
            Error::msg(format!(
                "host '{}' route '{}' directive 'js': {error}",
                context.host, context.prefix
            ))
        })
    }
}

fn parse(source: &serde_yaml::Value) -> Result<JavaScriptRouteConfig> {
    if source.as_mapping().is_some_and(|mapping| mapping.contains_key("path")) {
        return Err(Error::msg(
            "put the mount path in the routes map, not inside js",
        ));
    }
    serde_yaml::from_value(source.clone())
        .map_err(|error| Error::msg(error.to_string()))
}

pub(crate) fn register(state: &AppState) -> Result<()> {
    // Compiled support is not placement: a plain Axum consumer has no edge.
    if let Some(hosts) = state.registry_ref().resolve::<HostAppRegistry>() {
        hosts.register_route_extension(Arc::new(JsRouteExtension))?;
    }
    Ok(())
}

#[cfg(feature = "websocket")]
fn with_websockets(
    router: Router<AppState>,
    context: &RouteContext<'_, HostConfig>,
    config: &JavaScriptRouteConfig,
    service: Arc<JsApplicationService>,
) -> Result<Router<AppState>> {
    use axum::middleware;
    use axum::middleware::Next;
    use axum::response::IntoResponse;
    use claviron::websocket::{
        H1UpgradeMiddlewareState, H1UpgradeRegistry, h1_upgrade_middleware,
    };

    if config.websockets().is_empty() {
        return Ok(router);
    }
    if !context.config.transport.h1.upgrade {
        return Err(Error::msg(
            "WebSocket routes require transport.h1.upgrade: true",
        ));
    }
    // Owned by this candidate router, never inserted into a live global
    // registry. A failed reload leaves the previous bindings untouched.
    let registry = Arc::new(H1UpgradeRegistry::default());
    let handler = Arc::new(crate::JsWebSocketUpgradeHandler::new(
        service,
        config.application.as_str(),
    ));
    for websocket in config.websockets() {
        registry.register_handler(
            context.host.clone(),
            mounted_path(context.prefix, &websocket.path),
            handler.clone(),
        )?;
    }
    let bindings = H1UpgradeMiddlewareState {
        app_state: context.state.clone(),
        registry,
        host: context.host.clone(),
    };
    let h2_bindings = bindings.clone();
    let h2_enabled = context.config.transport.h2.connect_protocol;
    Ok(router
        .layer(middleware::from_fn_with_state(bindings, h1_upgrade_middleware))
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let bindings = h2_bindings.clone();
            let handler = handler.clone();
            async move {
                if request.version() != http::Version::HTTP_2
                    || request.method() != http::Method::CONNECT
                    || bindings
                        .registry
                        .resolve(&bindings.host, request.uri().path())
                        .is_none()
                {
                    return next.run(request).await;
                }
                if !h2_enabled {
                    return http::StatusCode::FORBIDDEN.into_response();
                }
                let (parts, body) = request.into_parts();
                into_axum_response(
                    handler
                        .handle_connect(
                            bindings.app_state,
                            http::Request::from_parts(parts, boxed_body(body)),
                        )
                        .await,
                )
            }
        })))
}

#[cfg(not(feature = "websocket"))]
fn with_websockets(
    router: Router<AppState>,
    _context: &RouteContext<'_, HostConfig>,
    config: &JavaScriptRouteConfig,
    _service: Arc<JsApplicationService>,
) -> Result<Router<AppState>> {
    if !config.websockets().is_empty() {
        return Err(Error::msg(
            "WebSocket routes require the websocket feature",
        ));
    }
    Ok(router)
}

#[cfg(feature = "websocket")]
fn mounted_path(
    mount: &claviron::http::ext::PathPrefix,
    path: &claviron::http::ext::PathPrefix,
) -> Arc<str> {
    match (mount.as_str(), path.as_str()) {
        ("/", path) => Arc::from(path),
        (mount, "/") => Arc::from(mount),
        (mount, path) => Arc::from(format!("{mount}{path}")),
    }
}

#[cfg(test)]
#[path = "../../tests/routing/edge.rs"]
mod tests;
