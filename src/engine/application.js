(application) => {
    const handler = typeof application === "function" ? application : application?.fetch;
    if (typeof handler !== "function") {
        throw new TypeError("JavaScript application must default-export a fetch function or an object with fetch(request)");
    }
    const runtime = globalThis[Symbol.for("claviron.runtime")];
    const http = globalThis[Symbol.for("claviron.http")](runtime);
    const requests = new Map();

    async function dispatch(id, bodyKind, data) {
        let failed = false;
        let error;
        try {
            const request = http.createRequest(id, bodyKind, data);
            const context = { request, reader: null };
            requests.set(id, context);
            let response = handler.call(application, request);
            if (!(response instanceof Response)) {
                response = await response;
                if (!(response instanceof Response)) {
                    throw new TypeError("JavaScript application fetch handler must return a Response");
                }
            }
            const source = http.respond(id, response);
            if (source === null) return;
            if (!(source instanceof ReadableStream)) {
                await source;
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
        } catch (cause) {
            failed = true;
            error = cause;
        } finally {
            try {
                requests.delete(id);
            } catch (cause) {
                failed = true;
                error = cause;
            }
            // These ops are installed once per isolate. Report only after
            // guest cleanup, including failures before Request construction.
            if (failed) runtime.failRequest(id, error);
            else runtime.completeRequest(id);
        }
    }

    globalThis[Symbol.for("claviron.application.dispatch")] = dispatch;
    globalThis[Symbol.for("claviron.application.cancel")] = (id) => {
        const context = requests.get(id);
        if (context === undefined) return;
        const reason = new DOMException("Request cancelled", "AbortError");
        http.abortRequest(context.request);
        // Cancellation also wakes a reader parked on a producer with no new data.
        if (context.reader !== null) {
            context.reader.cancel(reason).catch(() => {});
        }
    };
    globalThis[Symbol.for("claviron.application.shutdown")] = async () => {
        await application?.shutdown?.call(application);
    };
    globalThis[Symbol.for("claviron.application.drain")] = () => {
        for (const context of requests.values()) {
            // Signal owned work; keep accepted body I/O alive for its drain.
            http.abortRequest(context.request);
        }
    };
}
