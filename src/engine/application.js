(application) => {
    const handler = typeof application === "function" ? application : application?.fetch;
    if (typeof handler !== "function") {
        throw new TypeError("JavaScript application must default-export a fetch function or an object with fetch(request)");
    }
    const runtime = globalThis[Symbol.for("claviron.runtime")];
    const takeResponseBody = globalThis[Symbol.for("claviron.response.body")];
    const requests = new Map();

    async function dispatch(id) {
        const raw = JSON.parse(runtime.takeApplicationRequest(id));
        const abort = new AbortController();
        const context = { abort, reader: null };
        requests.set(id, context);
        try {
            let body;
            if (raw.streamed) {
                body = new ReadableStream({
                    async pull(controller) {
                        try {
                            const chunk = await runtime.readRequestBody(id);
                            if (chunk.length === 0) controller.close();
                            else controller.enqueue(chunk);
                        } catch (error) {
                            controller.error(error);
                        }
                    },
                });
            } else if (raw.has_body) {
                body = runtime.takeApplicationRequestBody(id);
            }
            const request = new Request(raw.url, {
                method: raw.method,
                headers: raw.headers,
                body,
                duplex: "half",
                signal: abort.signal,
            });
            const response = await handler.call(application, request);
            if (!(response instanceof Response)) {
                throw new TypeError("JavaScript application fetch handler must return a Response");
            }
            const head = JSON.stringify({ status: response.status, headers: Array.from(response.headers) });
            const source = takeResponseBody(response);
            if (!(source instanceof ReadableStream)) {
                runtime.respondWhole(id, head, source);
                return;
            }
            if (!runtime.respondHead(id, head)) {
                await source.cancel();
                return;
            }
            const reader = source.getReader();
            context.reader = reader;
            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    if (value.length !== 0 && !(await runtime.writeBody(id, value))) {
                        await reader.cancel();
                        break;
                    }
                }
            } finally {
                context.reader = null;
                reader.releaseLock();
            }
        } finally {
            requests.delete(id);
        }
    }

    globalThis[Symbol.for("claviron.application.dispatch")] = dispatch;
    globalThis[Symbol.for("claviron.application.cancel")] = (id) => {
        const context = requests.get(id);
        if (context === undefined) return;
        const reason = new DOMException("Request cancelled", "AbortError");
        context.abort.abort(reason);
        // Cancellation also wakes a reader parked on a producer with no new data.
        if (context.reader !== null) {
            context.reader.cancel(reason).catch(() => {});
        }
    };
    globalThis[Symbol.for("claviron.application.shutdown")] = async () => {
        await application?.shutdown?.call(application);
    };
    globalThis[Symbol.for("claviron.application.drain")] = () => {
        const reason = new DOMException("Application stopping", "AbortError");
        for (const context of requests.values()) {
            // Signal owned work; keep accepted body I/O alive for its drain.
            context.abort.abort(reason);
        }
    };
}
