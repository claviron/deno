use std::cell::{Cell, OnceCell, RefCell};
use std::rc::Rc;

use deno_core::{GarbageCollected, v8};

/// Request-owned values, independent of the live HTTP exchange. Retaining a
/// Web Request keeps these bytes, never an admission permit or an I/O endpoint.
pub(crate) struct RequestData {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
    body: RefCell<Option<Vec<u8>>>,
    external: Rc<Cell<i64>>,
    finalizer: OnceCell<v8::Weak<v8::Object>>,
}

impl RequestData {
    pub fn new(
        parts: http::request::Parts,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            body: RefCell::new(Some(body)),
            external: Rc::new(Cell::new(0)),
            finalizer: OnceCell::new(),
        }
    }

    pub fn into_v8<'s>(
        self,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> v8::Local<'s, v8::Object> {
        // Report buffered capacity and known metadata storage to V8. Native
        // buffers must contribute to GC pressure before JS materializes them.
        // HeaderMap/Uri expose payload lengths, not all allocator overhead.
        let mut size = self
            .body
            .borrow()
            .as_ref()
            .map_or(0, Vec::capacity)
            .saturating_add(self.method.as_str().len())
            .saturating_add(self.uri.scheme_str().map_or(0, str::len))
            .saturating_add(
                self.uri.authority().map_or(0, |v| v.as_str().len()),
            )
            .saturating_add(
                self.uri.path_and_query().map_or(0, |v| v.as_str().len()),
            )
            .saturating_add(self.headers.capacity().saturating_mul(
                std::mem::size_of::<(http::HeaderName, http::HeaderValue)>(),
            ));
        for (name, value) in &self.headers {
            size = size
                .saturating_add(name.as_str().len())
                .saturating_add(value.as_bytes().len());
        }
        let size = i64::try_from(size).unwrap_or(i64::MAX);
        self.external.set(size);
        let external = self.external.clone();
        let object = deno_core::cppgc::make_cppgc_object(scope, self);
        // The weak callback receives a live isolate; do not retain an isolate
        // pointer in Rust Drop or keep the Request alive through a strong handle.
        let finalizer = v8::Weak::with_finalizer(
            scope,
            object,
            Box::new(move |isolate| {
                isolate.adjust_amount_of_external_allocated_memory(
                    -external.replace(0),
                );
            }),
        );
        deno_core::cppgc::try_unwrap_cppgc_object::<Self>(scope, object.into())
            .expect("new RequestData has its native type")
            .finalizer
            .set(finalizer)
            .expect("RequestData finalizer is installed once");
        scope.adjust_amount_of_external_allocated_memory(size);
        object
    }

    pub fn take_body(
        &self,
        scope: &mut v8::PinScope,
    ) -> Result<Vec<u8>, deno_error::JsErrorBox> {
        let body = self.body.borrow_mut().take().ok_or_else(|| {
            deno_error::JsErrorBox::type_error(
                "request body was already consumed",
            )
        })?;
        // The op transfers these bytes to a V8 ArrayBuffer, which owns its own
        // accounting. Remove the native charge instead of counting them twice.
        let size = i64::try_from(body.capacity())
            .unwrap_or(i64::MAX)
            .min(self.external.get());
        self.external.set(self.external.get() - size);
        scope.adjust_amount_of_external_allocated_memory(-size);
        Ok(body)
    }
}

// SAFETY: fields own ordinary Rust values and an intentionally weak V8 handle.
// No strong V8 or cppgc references require tracing. The weak callback is owned
// by V8 and never holds a pointer back into this object's allocation.
unsafe impl GarbageCollected for RequestData {
    fn trace(
        &self,
        _visitor: &mut v8::cppgc::Visitor,
    ) {
    }

    fn get_name(&self) -> &'static std::ffi::CStr {
        c"ClavironRequestData"
    }
}

#[cfg(test)]
#[path = "../../tests/engine/request_data.rs"]
mod tests;
