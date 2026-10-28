let release;
let uploads = 0;
let uploadsReady;
let cancelled = false;
let onCancelled;
let waiting = false;
let onWaiting;
let ended = false;
let onEnded;
let failStream;
let drainPending = false;
let drainStarted;

export default {
    fetch(request) {
        const path = new URL(request.url).pathname;
        if (path === "/json") return Response.json({ ok: true });
        if (path === "/drain") {
            drainPending = true;
            drainStarted?.();
            return new Promise((resolve) => request.signal.addEventListener("abort", resolve, { once: true }))
                .then(() => { drainPending = false; return new Response("drained"); });
        }
        if (path === "/drain-started") {
            return (drainPending ? Promise.resolve() : new Promise((resolve) => { drainStarted = resolve; }))
                .then(() => new Response("started"));
        }
        if (path === "/echo") {
            uploads++;
            if (uploads === 2) uploadsReady?.();
            return request.arrayBuffer().then((body) => new Response(body, {
                headers: { "x-request": request.headers.get("x-request") },
            }));
        }
        if (path === "/uploads-ready") {
            return (uploads === 2 ? Promise.resolve() : new Promise((resolve) => { uploadsReady = resolve; }))
                .then(() => new Response("ready"));
        }
        if (path === "/flood") {
            return new Response(new ReadableStream({
                pull(controller) { controller.enqueue(new Uint8Array(1024)); },
                cancel() { cancelled = true; onCancelled?.(); },
            }));
        }
        if (path === "/cancelled") {
            return (cancelled ? Promise.resolve() : new Promise((resolve) => { onCancelled = resolve; }))
                .then(() => new Response("cancelled"));
        }
        if (path === "/waiting") {
            waiting = true;
            onWaiting?.();
            return new Promise((resolve) => {
                request.signal.addEventListener("abort", () => {
                    ended = true;
                    onEnded?.();
                    resolve(new Response("aborted"));
                }, { once: true });
            });
        }
        if (path === "/waiting-started") {
            return (waiting ? Promise.resolve() : new Promise((resolve) => { onWaiting = resolve; }))
                .then(() => new Response("started"));
        }
        if (path === "/waiting-ended") {
            return (ended ? Promise.resolve() : new Promise((resolve) => { onEnded = resolve; }))
                .then(() => new Response("ended"));
        }
        if (path === "/fail") {
            failStream();
            return new Response("failed");
        }
        if (path === "/failing-stream") {
            return new Response(new ReadableStream({
                start(controller) {
                    controller.enqueue(new TextEncoder().encode("prefix"));
                    failStream = () => controller.error(new Error("producer failed"));
                },
            }));
        }
        if (path === "/release") {
            release();
            release = undefined;
            return new Response("released");
        }
        if (path === "/stream") {
            return new Response(new ReadableStream({
                start(controller) {
                    controller.enqueue(new TextEncoder().encode("open"));
                    const close = () => {
                        request.signal.removeEventListener("abort", close);
                        release = undefined;
                        controller.enqueue(new TextEncoder().encode("closed"));
                        controller.close();
                    };
                    release = close;
                    request.signal.addEventListener("abort", close, { once: true });
                },
            }), { headers: { "content-type": request.headers.get("x-content-type") ?? "application/octet-stream" } });
        }
        return new Response("missing", { status: 404 });
    },
    shutdown() {
        if (release !== undefined || drainPending) {
            throw new Error("Application cleanup started before accepted requests drained");
        }
    },
};
