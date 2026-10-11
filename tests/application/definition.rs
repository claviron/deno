use std::path::PathBuf;

use super::ApplicationDefinition;
use crate::Error;

#[test]
fn rejects_relative_main_module() {
    let error =
        ApplicationDefinition::new("api", PathBuf::from("app.js")).unwrap_err();

    assert!(matches!(error, Error::InvalidModulePath(_)));
}

#[test]
fn freezes_explicit_application_environment() {
    let definition =
        ApplicationDefinition::new("api", PathBuf::from("/app.js"))
            .unwrap()
            .environment("APP_ENV", "production")
            .environments([(
                "API_URL".to_owned(),
                "https://example.test".to_owned(),
            )]);

    assert_eq!(definition.environment_values()["APP_ENV"], "production");
    assert_eq!(
        definition.environment_values()["API_URL"],
        "https://example.test"
    );
}

#[test]
fn resolved_values_do_not_replace_the_declared_recipe() {
    let declaration =
        ApplicationDefinition::new("example", PathBuf::from("/app.js"))
            .unwrap()
            .environment("VALUE", "${SOURCE}")
            .env_files([PathBuf::from("example.env")]);
    let resolved = declaration
        .clone()
        .resolved_environment([("VALUE".into(), "resolved".into())].into());
    assert_eq!(declaration.environment_values()["VALUE"], "${SOURCE}");
    assert_eq!(resolved.environment_values()["VALUE"], "resolved");
    assert_eq!(resolved.environment_files(), declaration.environment_files());
}
