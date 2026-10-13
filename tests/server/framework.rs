use super::*;

#[test]
fn detects_runtime_frameworks_without_counting_helpers_or_dev_dependencies() {
    for (package, expected) in [
        (
            r#"{"dependencies":{"hono":"*","@hono/node-server":"*"}}"#,
            vec!["Hono"],
        ),
        (
            r#"{"dependencies":{"hono":"*","elysia":"*"}}"#,
            vec!["Elysia", "Hono"],
        ),
        (
            r#"{"dependencies":{"hono":"*"},"peerDependencies":{"hono":"*"}}"#,
            vec!["Hono"],
        ),
        (r#"{"peerDependencies":{"@nestjs/core":"*"}}"#, vec!["NestJS"]),
        (
            r#"{"name":"hono-example","dependencies":{"hono-helper":"*"},"devDependencies":{"express":"*"}}"#,
            vec![],
        ),
    ] {
        assert_eq!(detect(package).expect("valid package JSON"), expected);
    }
}

#[test]
fn closest_package_owns_detection_and_explicit_framework_wins() {
    let root = tempfile::tempdir().expect("temporary application root");
    let source = root.path().join("src");
    std::fs::create_dir(&source).expect("source directory");
    std::fs::write(
        root.path().join("package.json"),
        r#"{"dependencies":{"hono":"*"}}"#,
    )
    .expect("owning package");
    let definition =
        ApplicationDefinition::new("example", source.join("app.ts")).unwrap();
    assert_eq!(resolve(&definition), ["Hono"]);
    assert!(
        resolve(&definition.clone().frameworks(Vec::<String>::new()))
            .is_empty()
    );

    let nested_package = source.join("package.json");
    std::fs::write(&nested_package, "{}").expect("nested package");
    assert!(resolve(&definition).is_empty());
    std::fs::write(&nested_package, "invalid JSON").expect("invalid package");
    assert!(resolve(&definition).is_empty());
    assert_eq!(
        resolve(&definition.frameworks(["Custom", "Private"])),
        ["Custom", "Private"]
    );
}
