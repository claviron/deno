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
        let (parts, body) = request.into_parts();
        let policy = parts
            .extensions
            .get::<HttpPolicy>()
            .copied()
            .unwrap_or(HttpPolicy::unlimited())
            .narrow(&self.route);
        // Whole or piece by piece, as the request policy says: the isolate
        // takes either, as a buffer or as the `Request`'s stream.
        let declared = http_body::Body::size_hint(&body).exact();
        let whole = http_body::Body::is_end_stream(&body)
            || policy
                .requests
                .delivery(
                    claviron::http::policy::BodyNeed::Stream,
                    declared,
                    false,
                )
                .takes_whole(declared);
        let body = if whole {
            crate::RequestBody::Whole(
                body.collect()
                    .await
                    .map_err(|error| {
                        crate::Error::ApplicationUnavailable(error.to_string())
                    })?
                    .to_bytes()
                    .to_vec(),
            )
        } else {
            crate::RequestBody::Stream(pump_request_body(body))
        };
        let url = application_url(&parts)?;
        let request = ApplicationRequest {
            method: parts.method.to_string(),
            url,
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect(),
            body,
        };
        let response = self
            .service
            .execute(&self.application, request, policy.requests.queue_timeout)
            .await?;
        let status =
            StatusCode::from_u16(response.status).map_err(|error| {
                crate::Error::ApplicationUnavailable(error.to_string())
            })?;
        let mut headers =
            http::HeaderMap::with_capacity(response.headers.len());
        for (name, value) in response.headers {
            let name = http::HeaderName::try_from(name).map_err(|error| {
                crate::Error::ApplicationUnavailable(error.to_string())
            })?;
            let value =
                http::HeaderValue::try_from(value).map_err(|error| {
                    crate::Error::ApplicationUnavailable(error.to_string())
                })?;
            headers.append(name, value);
        }
        // Held or passed on as it comes, as the reply's head and its policy
        // say — the same rule SAPI follows.
        let body = claviron::http::body::deliver_reply(
            &mut headers,
            boxed_body(ReplyBody(response.body)),
            &policy.responses,
        )
        .await;
        let mut reply = Response::new(body);
        *reply.status_mut() = status;
        *reply.headers_mut() = headers;
        Ok(reply)
    }
}

/// Fetch needs an absolute URL; the Rust request keeps its original URI.
pub(super) fn application_url(
    parts: &http::request::Parts
) -> crate::Result<String> {
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
    let path = parts.uri.path_and_query().map_or("/", |path| path.as_str());
    http::Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path)
        .build()
        .map(|uri| uri.to_string())
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
struct ReplyBody(tokio::sync::mpsc::Receiver<bytes::Bytes>);

impl http_body::Body for ReplyBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>,
    > {
        self.0
            .poll_recv(context)
            .map(|chunk| chunk.map(|chunk| Ok(http_body::Frame::data(chunk))))
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
        while let Some(frame) = body.frame().await {
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
