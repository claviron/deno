import { core, primordials } from "ext:core/mod.js";

const { SafeArrayIterator } = primordials;
const loadHeaders = () => core.loadExtScript("ext:deno_fetch/20_headers.js");
const loadFormData = () => core.loadExtScript("ext:deno_fetch/21_formdata.js");
const loadRequest = () => core.loadExtScript("ext:deno_fetch/23_request.js");
const loadBody = () => core.loadExtScript("ext:deno_fetch/22_body.js");
const loadResponse = () => core.loadExtScript("ext:deno_fetch/23_response.js");
const loadFetch = () => core.loadExtScript("ext:deno_fetch/26_fetch.js");

Object.defineProperty(globalThis, Symbol.for("claviron.http"), {
    value(runtime) {
        const { fromInnerRequest, abortRequest } = loadRequest();
        const { InnerBody } = loadBody();
        const {
            getInnerResponse, toInnerResponse, wireHeaderList, responseBodyUsed,
            serveFastStatusKey, serveFastBodyKey, serveFastHeaderKindKey,
            serveFastContentTypeKey, serveFastConsumedKey,
            SERVE_FAST_HEADER_DEFAULT_TEXT, SERVE_FAST_HEADER_CONTENT_TYPE,
        } = loadResponse();

        // Deno's branded Request owns Web API behavior, cloning and its lazy
        // AbortSignal. This inner target only reads Claviron-owned HTTP fields.
        class InnerRequest {
            #id;
            #data;
            #bodyKind;
            #method;
            #url;
            #headers;
            #body;
            redirectMode = "follow";
            redirectCount = 0;
            clientRid = null;
            blobUrlEntry = null;

            constructor(id, bodyKind, data) {
                this.#id = id;
                this.#bodyKind = bodyKind;
                this.#data = data;
            }
            get method() {
                if (this.#method === undefined) this.#method = runtime.requestMethod(this.#data);
                return this.#method;
            }
            url() {
                if (this.#url === undefined) this.#url = runtime.requestUrl(this.#data);
                return this.#url;
            }
            currentUrl() { return this.url(); }
            get headerList() {
                if (this.#headers === undefined) this.#headers = runtime.requestHeaders(this.#data);
                return this.#headers;
            }
            header(name) { return runtime.requestHeader(this.#data, name); }
            get body() {
                if (this.#body !== undefined) return this.#body;
                if (this.#bodyKind === 0 || this.method === "GET" ||
                    this.method === "HEAD" || this.method === "CONNECT") return this.#body = null;
                if (this.#bodyKind === 1) {
                    const body = runtime.takeApplicationRequestBody(this.#data);
                    this.#body = new InnerBody({ body, consumed: false });
                    this.#body.length = body.byteLength;
                    return this.#body;
                }
                const id = this.#id;
                return this.#body = new InnerBody(new ReadableStream({
                    async pull(controller) {
                        try {
                            const chunk = await runtime.readRequestBody(id);
                            if (chunk.length === 0) controller.close();
                            else controller.enqueue(chunk);
                        } catch (error) { controller.error(error); }
                    },
                }));
            }
        }

        return {
            createRequest(id, bodyKind, data) {
                return fromInnerRequest(new InnerRequest(id, bodyKind, data), "request");
            },
            abortRequest,
            respond(id, response) {
                const inner = getInnerResponse(response);
                if (inner === undefined || inner.type === "error") {
                    throw new TypeError("Application must return a valid Response");
                }
                if (responseBodyUsed(response)) throw new TypeError("Response body is already used");

                const status = response[serveFastStatusKey];
                const readyBody = response[serveFastBodyKey];
                // A body exposed as a stream may now be locked or have its
                // own reader. Only untouched/static bodies can use this path.
                if (status !== 0 && readyBody !== null &&
                    !(inner.body?.streamOrStatic instanceof ReadableStream)) {
                    const kind = response[serveFastHeaderKindKey];
                    const contentType = kind === SERVE_FAST_HEADER_DEFAULT_TEXT
                        ? "text/plain;charset=UTF-8"
                        : kind === SERVE_FAST_HEADER_CONTENT_TYPE
                        ? response[serveFastContentTypeKey] : null;
                    response[serveFastConsumedKey] = true;
                    runtime.respondWhole(id, status, contentType, readyBody);
                    return null;
                }

                // Preserve Fetch's decoded-body wire headers and duplicate
                // fields without constructing/iterating a public Headers view.
                toInnerResponse(response);
                const headers = wireHeaderList(inner);
                const body = inner.body;
                if (body === null) {
                    runtime.respondWhole(id, inner.status, headers, null);
                    return null;
                }
                if (body.unusable()) throw new TypeError("Response body is already used");
                const source = body.streamOrStatic;
                if (!(source instanceof ReadableStream)) {
                    source.consumed = true;
                    runtime.respondWhole(id, inner.status, headers, source.body);
                    return null;
                }
                if (!runtime.respondHead(id, inner.status, headers)) return source.cancel();
                return source;
            },
        };
    },
});

Object.defineProperties(globalThis, {
    Headers: core.propNonEnumerableLazyLoaded((module) => module.Headers, loadHeaders),
    FormData: core.propNonEnumerableLazyLoaded((module) => module.FormData, loadFormData),
    Request: core.propNonEnumerableLazyLoaded((module) => module.Request, loadRequest),
    Response: core.propNonEnumerableLazyLoaded((module) => module.Response, loadResponse),
    fetch: core.propWritable(function fetch(...args) {
        return loadFetch().fetch(...new SafeArrayIterator(args));
    }),
});
