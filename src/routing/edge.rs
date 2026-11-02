//! Compile the edge's `js:` directive into a native host contribution.

use std::sync::Arc;

use claviron::edge::HostAppRegistry;
use claviron::edge::pipeline::extension::{
    HostHandler, HostHandlerFuture, HostRequest, RouteContext, RouteEffect,
    RouteExtension,
};
use claviron::http::ServiceResult;
use claviron::http::policy::HttpPolicyOverride;
use claviron::runtime::AppState;
use claviron::runtime::registry::{Error, Result};

use super::JavaScriptRouteConfig;
use crate::{JsApplicationAdapter, JsApplicationService};

/// Bind a host route prefix to an application prepared from `js.yaml`.
/// Parsing and service resolution happen only during host preparation.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsRouteExtension;

impl RouteExtension for JsRouteExtension {
    fn name(&self) -> &'static str {
        "js"
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
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
            let adapter = JsApplicationAdapter::new(
                Arc::clone(&service),
                config.application.as_str(),
            )
            .route_policy(HttpPolicyOverride {
                requests: config.requests,
                responses: config.responses,
            });
            #[cfg(not(feature = "websocket"))]
            if !config.websockets().is_empty() {
                return Err(Error::msg(
                    "WebSocket routes require the websocket feature",
                ));
            }
            Ok(RouteEffect::Handler(Arc::new(JsHostHandler {
                adapter,
                #[cfg(feature = "websocket")]
                sockets: SocketRoutes::prepare(context, &config, service)?,
            })))
        };
        compile().map_err(|error| {
            Error::msg(format!(
                "host '{}' route '{}' directive 'js': {error}",
                context.host, context.prefix
            ))
        })
    }
}

struct JsHostHandler {
    adapter: JsApplicationAdapter,
    #[cfg(feature = "websocket")]
    sockets: Option<Arc<SocketRoutes>>,
}

impl HostHandler for JsHostHandler {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let adapter = self.adapter.clone();
        #[cfg(feature = "websocket")]
        let sockets = self.sockets.clone();
        Box::pin(async move {
            // Edge selected this prefix. Move the admitted request and unread
            // body directly to the application, without an Axum router.
            let request = request.into_request();
            #[cfg(feature = "websocket")]
            let request = if let Some(sockets) = sockets {
                match sockets.dispatch(request).await {
                    ServiceResult::Handled(response) => {
                        return ServiceResult::Handled(response);
                    }
                    ServiceResult::Unhandled(request) => request,
                }
            } else {
                request
            };
            ServiceResult::Handled(adapter.handle(request).await)
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
    if let Some(hosts) = state.registry_ref().resolve::<HostAppRegistry>() {
        hosts.register_route_extension(Arc::new(JsRouteExtension))?;
    }
    Ok(())
}

#[cfg(feature = "websocket")]
struct SocketRoutes {
    state: AppState,
    host: claviron::edge::Host,
    paths: std::collections::HashMap<
        Arc<str>,
        claviron::websocket::H1UpgradeBinding,
    >,
    handler: Arc<crate::JsWebSocketUpgradeHandler>,
    h2_enabled: bool,
}

#[cfg(feature = "websocket")]
impl SocketRoutes {
    fn prepare(
        context: &RouteContext<'_>,
        config: &JavaScriptRouteConfig,
        service: Arc<JsApplicationService>,
    ) -> Result<Option<Arc<Self>>> {
        if config.websockets().is_empty() {
            return Ok(None);
        }
        if !context.config.transport.h1.upgrade {
            return Err(Error::msg(
                "WebSocket routes require transport.h1.upgrade: true",
            ));
        }
        let registry = claviron::websocket::H1UpgradeRegistry::default();
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
        Ok(Some(Arc::new(Self {
            state: context.state.clone(),
            host: context.host.clone(),
            paths: registry.snapshot_for_host(context.host),
            handler,
            h2_enabled: context.config.transport.h2.connect_protocol,
        })))
    }

    async fn dispatch(
        &self,
        request: http::Request<claviron::http::RouteBody>,
    ) -> ServiceResult<
        claviron::http::RouteResponse,
        http::Request<claviron::http::RouteBody>,
    > {
        let Some(binding) = self.paths.get(request.uri().path()) else {
            return ServiceResult::Unhandled(request);
        };
        if request.version() == http::Version::HTTP_2
            && request.method() == http::Method::CONNECT
        {
            return ServiceResult::Handled(if self.h2_enabled {
                self.handler.handle_connect(self.state.clone(), request).await
            } else {
                claviron::http::err_status(http::StatusCode::FORBIDDEN)
            });
        }
        claviron::websocket::try_h1_upgrade(
            self.state.clone(),
            self.host.clone(),
            binding.clone(),
            request,
        )
        .await
    }
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
