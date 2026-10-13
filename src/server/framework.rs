use crate::ApplicationDefinition;

/// Resolve management metadata once, while registering the application.
/// The nearest package owns the entry module; never inspect node_modules or
/// execute JavaScript to identify its framework.
pub(super) fn resolve(definition: &ApplicationDefinition) -> Vec<String> {
    if let Some(frameworks) = definition.framework_names() {
        return frameworks.to_vec();
    }

    for directory in definition
        .main_module()
        .parent()
        .into_iter()
        .flat_map(std::path::Path::ancestors)
    {
        let path = directory.join("package.json");
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => {
                tracing::warn!(application = definition.name(), path = %path.display(), %error, "framework metadata could not be read");
                return Vec::new();
            }
        };
        // Stop at the owning package even if it has no recognized framework.
        return match detect(&text) {
            Ok(frameworks) => {
                frameworks.into_iter().map(str::to_owned).collect()
            }
            Err(error) => {
                tracing::warn!(application = definition.name(), path = %path.display(), %error, "framework metadata could not be parsed");
                Vec::new()
            }
        };
    }
    Vec::new()
}

fn detect(text: &str) -> Result<Vec<&'static str>, serde_json::Error> {
    let package: serde_json::Value = serde_json::from_str(text)?;
    let detected = ["dependencies", "peerDependencies"]
        .into_iter()
        .filter_map(|section| package.get(section)?.as_object())
        .flat_map(|dependencies| dependencies.keys())
        .filter_map(|name| known_framework(name));
    let mut labels = Vec::new();
    for label in detected {
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    // Object iteration may differ with serde_json's preserve_order feature.
    labels.sort_unstable();
    Ok(labels)
}

fn known_framework(package: &str) -> Option<&'static str> {
    match package.to_ascii_lowercase().as_str() {
        "hono" => Some("Hono"),
        "elysia" => Some("Elysia"),
        "express" => Some("Express"),
        "fastify" => Some("Fastify"),
        "koa" => Some("Koa"),
        "@nestjs/core" => Some("NestJS"),
        "@hapi/hapi" => Some("Hapi"),
        _ => None,
    }
}

#[cfg(test)]
#[path = "../../tests/server/framework.rs"]
mod tests;
