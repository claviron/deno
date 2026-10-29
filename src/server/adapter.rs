use std::sync::Arc;

use claviron::http::ext::request_scheme;
use claviron::http::policy::{HttpPolicy, HttpPolicyOverride};
use claviron::http::{RouteBody, RouteResponse, boxed_body, err_status};
use http::header::{HeaderValue, RETRY_AFTER};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;

use super::JsApplicationService;
use crate::{ApplicationRequest, Error};

#[derive(Clone)]
pub struct JsApplicationAdapter {
    service: Arc<JsApplicationService>,
    application: Arc<str>,
    /// The route's `requests:` / `responses:`, narrowing what the edge hands
    /// over. Empty for a caller routing on its own.
    route: HttpPolicyOverride,
}

impl JsApplicationAdapter {
    pub fn new(
        service: Arc<JsApplicationService>,
        application: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            service,
            application: application.into(),
            route: HttpPolicyOverride::default(),
        }
    }

    /// The route's own policy blocks.
    pub fn route_policy(
        mut self,
        route: HttpPolicyOverride,
    ) -> Self {
        self.route = route;
        self
    }

    pub async fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteResponse {
        match self.try_handle(request).await {
            Ok(response) => response,
            Err(error) => {
                let status = status_for_error(&error);
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    tracing::warn!(application = %self.application, %error, "JavaScript application is unavailable");
                } else {
                    tracing::error!(application = %self.application, %error, "JavaScript application request failed");
                }
                error_response(status)
            }
        }
    }

    async fn try_handle(
        &self,
        request: Request<RouteBody>,
    ) -> crate::Result<RouteResponse> {
        let (mut parts, body) = request.into_parts();
        let policy = parts
            .extensions
            .get::<HttpPolicy>()
            .copied()
            .unwrap_or(HttpPolicy::unlimited())
            .narrow(&self.route);
        // Fetch GET/HEAD never carry a request body. Do not collect, size,
        // guard or pump one here. Framing remains the HTTP transport's job.
        let body = if parts.method == http::Method::GET
            || parts.method == http::Method::HEAD
            || http_body::Body::is_end_stream(&body)
        {
            crate::RequestBody::Empty
        } else {
            let declared = http_body::Body::size_hint(&body).exact();
            if policy
                .requests
                .delivery(
                    claviron::http::policy::BodyNeed::Stream,
                    declared,
                    false,
                )
                .takes_whole(declared)
            {
                crate::RequestBody::Whole(
                    body.collect()
                        .await
                        .map_err(|error| {
                            crate::Error::ApplicationUnavailable(
                                error.to_string(),
                            )
                        })?
                        .to_bytes()
                        .to_vec(),
                )
            } else {
                crate::RequestBody::Stream(pump_request_body(body))
            }
        };
        parts.uri = application_uri(&parts)?;
        let request = ApplicationRequest::from_parts(parts, body);
        let response = self
            .service
            .execute(&self.application, request, policy.requests.queue_timeout)
            .await?;
        let (mut parts, body) = response.into_parts();
        let body = match body {
            crate::ResponseBody::Whole(bytes) => {
                parts.headers.remove(claviron::http::policy::X_ACCEL_BUFFERING);
                claviron::http::bytes_body(bytes)
            }
            crate::ResponseBody::Stream(stream) => {
                claviron::http::body::deliver_reply(
                    &mut parts.headers,
                    boxed_body(ReplyBody(stream)),
                    &policy.responses,
                )
                .await
            }
        };
        Ok(Response::from_parts(parts, body))
    }
}

/// Resolve Fetch's absolute URI while preserving the accepted path and query.
pub(super) fn application_uri(
    parts: &http::request::Parts
) -> crate::Result<http::Uri> {
    use http::uri::{Authority, Scheme};

    let scheme = request_scheme(&parts.extensions)
        .or_else(|| {
            parts
                .uri
                .scheme()
                .filter(|scheme| {
                    **scheme == Scheme::HTTP || **scheme == Scheme::HTTPS
                })
                .cloned()
        })
        .ok_or_else(|| {
            Error::ApplicationUnavailable(
                "request has no HTTP transport scheme".into(),
            )
        })?;
    let authority = parts
        .uri
        .authority()
        .cloned()
        .or_else(|| {
            parts
                .headers
                .get(http::header::HOST)
                .and_then(|host| host.to_str().ok())
                .and_then(|host| host.parse::<Authority>().ok())
        })
        .filter(|authority| !authority.as_str().contains('@'))
        .ok_or_else(|| {
            Error::ApplicationUnavailable(
                "request has no valid authority".into(),
            )
        })?;
    let path = parts
        .uri
        .path_and_query()
        .cloned()
        .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/"));
    http::Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path)
        .build()
        .map_err(|error| Error::ApplicationUnavailable(error.to_string()))
}

fn status_for_error(error: &Error) -> StatusCode {
    match error {
        Error::ApplicationNotRunning(_)
        | Error::RequestChannelClosed
        | Error::AdmissionFull
        | Error::DispatchQueueTimedOut(_) => StatusCode::SERVICE_UNAVAILABLE,
        Error::ExecutionTimedOut(_) => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_response(status: StatusCode) -> RouteResponse {
    let mut response = err_status(status);
    if status == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

#[cfg(test)]
#[path = "../../tests/server/adapter.rs"]
mod tests;

/// A reply's body as the isolate writes it, one piece per `writeBody`. Ends
/// when the handler finishes and its sender is dropped.
struct ReplyBody(crate::ResponseStream);

impl http_body::Body for ReplyBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>,
    > {
        self.0.poll_recv(context).map(|chunk| {
            chunk.map(|chunk| {
                chunk.map(http_body::Frame::data).map_err(std::io::Error::other)
            })
        })
    }
}

/// Pieces of a request body in flight to the isolate. Small, as the reply's:
/// a handler that reads slowly is felt by the client as backpressure.
const REQUEST_BODY_CAPACITY: usize = 2;

/// Hand a request body to the isolate as it arrives. A read that fails is
/// sent on as an error, so the handler's stream fails instead of ending as if
/// whole; a handler that stops reading ends the pump with its request.
fn pump_request_body(
    mut body: RouteBody
) -> tokio::sync::mpsc::Receiver<std::result::Result<bytes::Bytes, String>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(REQUEST_BODY_CAPACITY);
    tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                biased;
                _ = sender.closed() => return,
                frame = body.frame() => frame,
            };
            let Some(frame) = frame else { return };
            let sent = match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(data) if data.is_empty() => continue,
                    Ok(data) => sender.send(Ok(data)).await,
                    Err(_) => continue,
                },
                Err(error) => {
                    let _ = sender.send(Err(error.to_string())).await;
                    return;
                }
            };
            if sent.is_err() {
                return;
            }
        }
    });
    receiver
}
