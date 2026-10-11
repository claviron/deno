//! Host-independent preparation and dispatch of JavaScript application routes.
//! Host selection and layer ordering relative to other engines belong to the caller.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, middleware};
use claviron::http::ext::PathPrefix;
use claviron::http::{boxed_body, into_axum_response};
use claviron::runtime::AppState;

use crate::{JsApplicationAdapter, JsApplicationService};

mod config;
pub use config::{JavaScriptRouteConfig, JavaScriptWebSocketConfig};

#[cfg(feature = "edge")]
pub mod edge;

/// Prepare configured mounts once, with the most specific prefix outermost.
pub fn build_router(
    router: Router,
    state: &AppState,
    javascript: &[JavaScriptRouteConfig],
) -> Router {
    let mut applications = javascript.to_vec();
    applications.sort_by(|a, b| match (&a.path, &b.path) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
    });
    let mut router = router;
    for application in applications {
        router = build_application_router(
            router,
            state,
            application.path,
            application.application,
            claviron::http::policy::HttpPolicyOverride {
                requests: application.requests,
                responses: application.responses,
            },
        );
    }
    router
}

#[derive(Clone)]
struct ApplicationRoute {
    path: Option<PathPrefix>,
    adapter: Option<JsApplicationAdapter>,
}

async fn dispatch(
    State(route): State<ApplicationRoute>,
    request: Request,
    next: Next,
) -> Response {
    if route
        .path
        .as_ref()
        .is_some_and(|prefix| !prefix.has_path_segment(request.uri().path()))
    {
        return next.run(request).await;
    }
    let Some(adapter) = route.adapter else {
        return (
            http::StatusCode::SERVICE_UNAVAILABLE,
            [(http::header::RETRY_AFTER, "1")],
        )
            .into_response();
    };
    let (parts, body) = request.into_parts();
    into_axum_response(
        adapter
            .handle(http::Request::from_parts(parts, boxed_body(body)))
            .await,
    )
}

/// Mount one named persistent JavaScript application at a segment-safe prefix.
pub fn build_application_router(
    base: Router,
    state: &AppState,
    path: Option<PathPrefix>,
    application: impl Into<std::sync::Arc<str>>,
    route: claviron::http::policy::HttpPolicyOverride,
) -> Router {
    let application = application.into();
    let adapter = state.registry_ref().resolve::<JsApplicationService>().and_then(|service| {
        if !service.is_configured(&application) {
            tracing::error!(application = %application, "JavaScript route names an application absent from js.yaml");
            None
        } else {
            Some(JsApplicationAdapter::new(service, Arc::clone(&application)).route_policy(route))
        }
    });
    base.layer(middleware::from_fn_with_state(
        ApplicationRoute { path, adapter },
        dispatch,
    ))
}
