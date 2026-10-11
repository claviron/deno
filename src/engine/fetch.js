import { core, primordials } from "ext:core/mod.js";

const { SafeArrayIterator } = primordials;
const loadHeaders = () => core.loadExtScript("ext:deno_fetch/20_headers.js");
const loadFormData = () => core.loadExtScript("ext:deno_fetch/21_formdata.js");
const loadRequest = () => core.loadExtScript("ext:deno_fetch/23_request.js");
const loadResponse = () => core.loadExtScript("ext:deno_fetch/23_response.js");
const loadFetch = () => core.loadExtScript("ext:deno_fetch/26_fetch.js");

Object.defineProperties(globalThis, {
    Headers: core.propNonEnumerableLazyLoaded((module) => module.Headers, loadHeaders),
    FormData: core.propNonEnumerableLazyLoaded((module) => module.FormData, loadFormData),
    Request: core.propNonEnumerableLazyLoaded((module) => module.Request, loadRequest),
    Response: core.propNonEnumerableLazyLoaded((module) => module.Response, loadResponse),
    fetch: core.propWritable(function fetch(...args) {
        return loadFetch().fetch(...new SafeArrayIterator(args));
    }),
});
