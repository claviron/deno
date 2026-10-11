import { core } from "ext:core/mod.js";

const {
    op_claviron_application_name,
    op_claviron_runtime_available,
    op_claviron_is_cancelled,
    op_claviron_cancelled,
    op_claviron_environment,
    op_claviron_runtime_yield,
    op_claviron_application_request,
    op_claviron_application_request_body,
    op_claviron_application_request_read,
    op_claviron_application_response_head,
    op_claviron_application_response_write,
} = core.ops;

const runtime = Object.freeze({
    applicationName() {
        return op_claviron_application_name();
    },

    runtimeAvailable() {
        return op_claviron_runtime_available();
    },

    isCancelled() {
        return op_claviron_is_cancelled();
    },

    cancelled() {
        return op_claviron_cancelled();
    },

    environment(name) {
        return op_claviron_environment(String(name));
    },

    yieldToRuntime(value) {
        return op_claviron_runtime_yield(value);
    },

    takeApplicationRequest() {
        return op_claviron_application_request();
    },

    takeApplicationRequestBody() {
        return op_claviron_application_request_body();
    },

    readRequestBody() {
        return op_claviron_application_request_read();
    },

    respondHead(head) {
        op_claviron_application_response_head(head);
    },

    writeBody(chunk) {
        return op_claviron_application_response_write(chunk);
    },
});

Object.defineProperty(globalThis, Symbol.for("claviron.runtime"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: runtime,
});
