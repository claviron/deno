use claviron::helpers::de::opt_one_or_many;
use claviron::http::ext::PathPrefix;
use claviron::http::policy::{RequestPolicyOverride, ResponsePolicyOverride};
use serde::Deserialize;

/// A mount of a named application from `js.yaml`; it does not define an engine.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JavaScriptRouteConfig {
    pub path: Option<PathPrefix>,
    #[serde(deserialize_with = "application_name")]
    pub application: String,
    #[serde(
        default,
        alias = "websocket",
        deserialize_with = "opt_one_or_many"
    )]
    websockets: Option<Vec<JavaScriptWebSocketConfig>>,
    #[serde(default)]
    pub requests: RequestPolicyOverride,
    #[serde(default)]
    pub responses: ResponsePolicyOverride,
}

impl JavaScriptRouteConfig {
    /// Exact WebSocket endpoints relative to the application's mount.
    pub fn websockets(&self) -> &[JavaScriptWebSocketConfig] {
        self.websockets.as_deref().unwrap_or(&[])
    }
}

/// One exact WebSocket endpoint, relative to the optional application prefix.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JavaScriptWebSocketConfig {
    pub path: PathPrefix,
}

fn application_name<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = String::deserialize(deserializer)?;
    if name.trim().is_empty() {
        return Err(serde::de::Error::custom(
            "JavaScript route application must not be empty",
        ));
    }
    Ok(name)
}
