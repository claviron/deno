(() => {
    const encoder = {
        encode(value) {
            const escaped = unescape(encodeURIComponent(String(value)));
            return Uint8Array.from(escaped, (character) => character.charCodeAt(0));
        },
    };

    const decoder = {
        decode(value) {
            const bytes = value instanceof Uint8Array ? value : new Uint8Array(value ?? 0);
            let encoded = "";
            for (const byte of bytes) encoded += String.fromCharCode(byte);
            return decodeURIComponent(escape(encoded));
        },
    };

    class Headers {
        #values = new Map();

        constructor(init = undefined) {
            if (init instanceof Headers) {
                for (const [name, value] of init) this.append(name, value);
            } else if (Array.isArray(init)) {
                for (const pair of init) this.append(pair[0], pair[1]);
            } else if (init && typeof init === "object") {
                for (const [name, value] of Object.entries(init)) this.append(name, value);
            }
        }

        #name(name) {
            return String(name).trim().toLowerCase();
        }

        append(name, value) {
            name = this.#name(name);
            value = String(value).trim();
            const current = this.#values.get(name);
            this.#values.set(name, current === undefined ? value : `${current}, ${value}`);
        }

        delete(name) { this.#values.delete(this.#name(name)); }
        get(name) { return this.#values.get(this.#name(name)) ?? null; }
        has(name) { return this.#values.has(this.#name(name)); }
        set(name, value) { this.#values.set(this.#name(name), String(value).trim()); }
        entries() { return this.#values.entries(); }
        keys() { return this.#values.keys(); }
        values() { return this.#values.values(); }
        forEach(callback, thisArg = undefined) {
            for (const [name, value] of this.#values) callback.call(thisArg, value, name, this);
        }
        [Symbol.iterator]() { return this.entries(); }
    }

    function bodyBytes(body) {
        if (body === null || body === undefined) return new Uint8Array();
        if (body instanceof Uint8Array) return body.slice();
        if (body instanceof ArrayBuffer) return new Uint8Array(body.slice(0));
        if (ArrayBuffer.isView(body)) {
            return new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice();
        }
        return encoder.encode(body);
    }

    class Body {
        #bytes;
        bodyUsed = false;

        constructor(body) { this.#bytes = body instanceof ReadableStream ? body : bodyBytes(body); }
        get body() { return this.#bytes; }
        async arrayBuffer() {
            if (this.bodyUsed) throw new TypeError("Body already consumed");
            this.bodyUsed = true;
            if (this.#bytes instanceof ReadableStream) {
                const reader = this.#bytes.getReader();
                const chunks = [];
                let size = 0;
                try {
                    while (true) {
                        const { done, value } = await reader.read();
                        if (done) break;
                        chunks.push(value);
                        size += value.length;
                    }
                } finally { reader.releaseLock(); }
                const bytes = new Uint8Array(size);
                let offset = 0;
                for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
                return bytes.buffer;
            }
            return this.#bytes.buffer.slice(
                this.#bytes.byteOffset,
                this.#bytes.byteOffset + this.#bytes.byteLength,
            );
        }
        async text() { return decoder.decode(await this.arrayBuffer()); }
        async json() { return JSON.parse(await this.text()); }
        _copyBody() { return this.#bytes.slice(); }
        _takeBody() {
            if (this.bodyUsed) throw new TypeError("Body already consumed");
            this.bodyUsed = true;
            return this.#bytes;
        }
    }

    class Request extends Body {
        constructor(input, init = {}) {
            const source = input instanceof Request ? input : null;
            super(init.body ?? source?._copyBody());
            this.url = String(source?.url ?? input);
            this.method = String(init.method ?? source?.method ?? "GET").toUpperCase();
            this.headers = new Headers(init.headers ?? source?.headers);
            this.redirect = init.redirect ?? source?.redirect ?? "follow";
            this.credentials = init.credentials ?? source?.credentials ?? "same-origin";
            this.signal = init.signal ?? source?.signal ?? new AbortController().signal;
        }
        clone() { return new Request(this); }
    }

    class Response extends Body {
        constructor(body = null, init = {}) {
            super(body);
            this.status = Number(init.status ?? 200);
            this.statusText = String(init.statusText ?? "");
            this.headers = new Headers(init.headers);
            this.ok = this.status >= 200 && this.status <= 299;
            this.redirected = false;
            this.type = "default";
            this.url = "";
        }
        clone() {
            return new Response(this._copyBody(), {
                status: this.status,
                statusText: this.statusText,
                headers: this.headers,
            });
        }
        static json(value, init = {}) {
            const headers = new Headers(init.headers);
            if (!headers.has("content-type")) headers.set("content-type", "application/json");
            return new Response(JSON.stringify(value), { ...init, headers });
        }
        static redirect(url, status = 302) {
            return new Response(null, { status, headers: { location: String(url) } });
        }
    }

    Object.assign(globalThis, { Headers, Request, Response });
    Object.defineProperty(globalThis, Symbol.for("claviron.http"), {
        value(runtime) {
            const controllers = new WeakMap();
            return {
                createRequest(id, bodyKind, data) {
                    const abort = new AbortController();
                    const body = bodyKind === 2 ? new ReadableStream({
                        async pull(controller) {
                            try {
                                const chunk = await runtime.readRequestBody(id);
                                if (chunk.length === 0) controller.close();
                                else controller.enqueue(chunk);
                            } catch (error) { controller.error(error); }
                        },
                    }) : bodyKind === 1 ? runtime.takeApplicationRequestBody(data) : undefined;
                    const request = new Request(runtime.requestUrl(data), {
                        method: runtime.requestMethod(data), headers: runtime.requestHeaders(data),
                        body, signal: abort.signal,
                    });
                    controllers.set(request, abort);
                    return request;
                },
                abortRequest(request) {
                    controllers.get(request).abort(new DOMException("Request cancelled", "AbortError"));
                },
                respond(id, response) {
                    const headers = Array.from(response.headers);
                    const body = response._takeBody();
                    if (!(body instanceof ReadableStream)) {
                        runtime.respondWhole(id, response.status, headers, body);
                        return null;
                    }
                    if (!runtime.respondHead(id, response.status, headers)) return body.cancel();
                    return body;
                },
            };
        },
    });
})();
