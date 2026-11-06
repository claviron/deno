//! Prepare the edge's `js:` backend without an application router wrapper.

use std::sync::Arc;

use claviron::edge::HostAppRegistry;
use claviron::http::policy::HttpPolicyOverride;
use claviron::http::{RouteBody, ServiceResult};
use claviron::pipeline::factory::{
    BackendContext, BackendFactory, BackendPlacement,
};
use claviron::pipeline::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchCall, DispatchStep, HostContext,
};
use claviron::runtime::AppState;
use claviron::runtime::registry::{Error, Result};
use http::Request;

use super::JavaScriptRouteConfig;
use crate::{JsApplicationAdapter, JsApplicationService};

/// Prepare a named JavaScript application after native Static/TryFiles.
/// The pipeline exports request identity and policy at the backend handoff.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsBackendFactory;

impl BackendFactory for JsBackendFactory {
    fn name(&self) -> &'static str {
        "js"
    }

    fn placement(&self) -> BackendPlacement {
        BackendPlacement::AfterFiles
    }

    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>> {
        let Some(source) =
            context.route.directive("js").filter(|value| !value.is_null())
        else {
            return Ok(Vec::new());
        };
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
            let mut owners: Vec<Arc<dyn BackendHandler>> = Vec::new();
            #[cfg(feature = "websocket")]
            if let Some(sockets) =
                SocketRoutes::prepare(context, &config, service)?
            {
                owners.push(Arc::new(sockets));
            }
            owners.push(Arc::new(JsBackend { adapter }));
            Ok(owners)
        };
        compile().map_err(|error| match context.host {
            Some(host) => Error::msg(format!(
                "host '{host}' route '{}' directive 'js': {error}",
                context.prefix
            )),
            None => Error::msg(format!(
                "route '{}' directive 'js': {error}",
                context.prefix
            )),
        })
    }
}

struct JsBackend {
    adapter: JsApplicationAdapter,
}

impl DispatchStep for JsBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        let adapter = self.adapter.clone();
        DispatchCall::new(async move {
            ServiceResult::Handled(adapter.handle(request).await)
        })
    }
}

impl BackendHandler for JsBackend {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
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
        hosts.register_backend_factory(Arc::new(JsBackendFactory))?;
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
    h1_enabled: bool,
    h2_enabled: bool,
}

#[cfg(feature = "websocket")]
impl SocketRoutes {
    fn prepare(
        context: &BackendContext<'_>,
        config: &JavaScriptRouteConfig,
        service: Arc<JsApplicationService>,
    ) -> Result<Option<Self>> {
        if config.websockets().is_empty()
            || context.protocol
                == Some(claviron::pipeline::factory::RouteProtocol::Quic)
        {
            return Ok(None);
        }
        let h1_enabled = context.transport.h1.upgrade;
        let h2_enabled = context.transport.h2.connect_protocol;
        if !h1_enabled && !h2_enabled {
            return Err(Error::msg(
                "WebSocket routes require transport.h1.upgrade or transport.h2.connect_protocol",
            ));
        }
        let host = context.host.ok_or_else(|| Error::msg(
            "WebSocket routes require a virtual host for native session bindings",
        ))?;
        let registry = claviron::websocket::H1UpgradeRegistry::default();
        let handler = Arc::new(crate::JsWebSocketUpgradeHandler::new(
            service,
            config.application.as_str(),
        ));
        for websocket in config.websockets() {
            registry.register_handler(
                host.clone(),
                mounted_path(context.prefix, &websocket.path),
                handler.clone(),
            )?;
        }
        Ok(Some(Self {
            state: context.state.clone(),
            host: host.clone(),
            paths: registry.snapshot_for_host(host),
            handler,
            h1_enabled,
            h2_enabled,
        }))
    }
}

#[cfg(feature = "websocket")]
impl DispatchStep for SocketRoutes {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        let Some(binding) = self.paths.get(request.uri().path()) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        if request.version() == http::Version::HTTP_2
            && request.method() == http::Method::CONNECT
        {
            if !self.h2_enabled {
                return DispatchCall::ready(ServiceResult::Handled(
                    claviron::http::err_status(http::StatusCode::FORBIDDEN),
                ));
            }
            let handler = Arc::clone(&self.handler);
            let state = self.state.clone();
            return DispatchCall::new(async move {
                ServiceResult::Handled(
                    handler.handle_connect(state, request).await,
                )
            });
        }
        if !self.h1_enabled {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        }
        let state = self.state.clone();
        let host = self.host.clone();
        let binding = binding.clone();
        DispatchCall::new(async move {
            claviron::websocket::try_h1_upgrade(state, host, binding, request)
                .await
        })
    }
}

#[cfg(feature = "websocket")]
impl BackendHandler for SocketRoutes {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::Request,
            reply: BackendReply::Owner,
        }
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
