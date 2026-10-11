#[cfg(feature = "fetch")]
use std::sync::Arc;

use deno_core::Extension;
#[cfg(feature = "fetch")]
use deno_permissions::{
    Permissions, PermissionsContainer, PermissionsOptions,
    RuntimePermissionDescriptorParser,
};

use crate::ExtensionContext;
use crate::backend::deno::{self, SharedExtensionFactory};

#[cfg(feature = "fetch")]
struct StaticRootCertStore(Arc<deno_tls::rustls::RootCertStore>);

#[cfg(feature = "fetch")]
impl deno_tls::RootCertStoreProvider for StaticRootCertStore {
    fn get_or_try_init(
        &self
    ) -> Result<&deno_tls::rustls::RootCertStore, deno_error::JsErrorBox> {
        Ok(&self.0)
    }
}

deno_core::extension!(
    claviron_web_platform,
    deps = [deno_web, deno_crypto],
    esm_entry_point = "ext:claviron_web_platform/platform.js",
    esm = [dir "src/engine", "platform.js"],
);

#[cfg(feature = "fetch")]
deno_core::extension!(
    claviron_fetch_platform,
    deps = [deno_fetch],
    esm_entry_point = "ext:claviron_fetch_platform/fetch.js",
    esm = [dir "src/engine", "fetch.js"],
    state = |state| {
        let parser = RuntimePermissionDescriptorParser::new(sys_traits::impls::RealSys);
        let permissions = Permissions::from_options(
            &parser,
            &PermissionsOptions { allow_net: Some(Vec::new()), ..Default::default() },
        )
        .expect("the built-in Fetch permission policy is valid");
        state.put(PermissionsContainer::new(Arc::new(parser), permissions));
    },
);

/// Builds the slim engine's ordered platform extension set.
///
/// Deno platform foundations must precede Claviron-owned extensions. Keep
/// this composition private to the slim engine: the full Deno backend already
/// owns its platform extensions and consumes only `backend::deno::extensions`.
pub(crate) fn runtime_extensions(
    context: ExtensionContext,
    factories: &[SharedExtensionFactory],
) -> Vec<Extension> {
    #[cfg(feature = "fetch")]
    let root_cert_store_provider =
        context.fetch_root_cert_store().map(|store| {
            Arc::new(StaticRootCertStore(store))
                as Arc<dyn deno_tls::RootCertStoreProvider>
        });
    let mut extensions = vec![
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
    ];
    #[cfg(feature = "fetch")]
    extensions.push(deno_fetch::deno_fetch::init(deno_fetch::Options {
        user_agent: context.fetch_user_agent().to_owned(),
        root_cert_store_provider,
        ..Default::default()
    }));
    #[cfg(feature = "fetch")]
    extensions.push(deno_net::deno_net::init(None, None));
    #[cfg(feature = "fetch")]
    extensions.push(deno_tls::deno_tls::init());
    extensions.push(claviron_web_platform::init());
    #[cfg(feature = "fetch")]
    extensions.push(claviron_fetch_platform::init());
    #[cfg(feature = "websocket")]
    extensions.push(crate::extensions::websocket::extension());
    extensions.extend(deno::extensions(context.clone()));
    extensions.extend(
        factories.iter().map(|factory| factory.extension(context.clone())),
    );
    extensions
}
