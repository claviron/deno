use deno_core::Extension;

/// Shared by the build script and runtime so their platform sources stay aligned.
pub(super) fn extensions(
    #[cfg(feature = "fetch")] fetch: deno_fetch::Options
) -> Vec<Extension> {
    vec![
        #[cfg(feature = "fetch")]
        deno_telemetry::deno_telemetry::init(),
        deno_webidl::deno_webidl::init(),
        deno_web::deno_web::init(
            deno_web::BlobStore::default_arc(),
            None,
            Default::default(),
            Default::default(),
        ),
        deno_crypto::deno_crypto::init(None),
        #[cfg(feature = "fetch")]
        deno_fetch::deno_fetch::init(fetch),
        #[cfg(feature = "fetch")]
        deno_net::deno_net::init(None, None),
        #[cfg(feature = "fetch")]
        deno_tls::deno_tls::init(),
    ]
}
