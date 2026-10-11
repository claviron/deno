const core = globalThis.Deno.core;
const { revive } = globalThis[Symbol.for("claviron.orbit.cell")];
const handlers = new Map();

function normalizedOperation(operation) {
    const value = String(operation).trim();
    if (value.length === 0) {
        throw new TypeError("Orbit invocation operation cannot be empty");
    }
    return value;
}

// Publish an invocation and return its id. Nothing waits: whoever subscribed
// to the operation, on any worker and in any language, takes it from there,
// and an outcome, if the consumer publishes one, is an ordinary application
// event keyed by this id.
function send(operation, payload = null) {
    return core.ops.op_claviron_orbit_invoke_send({
        operation: normalizedOperation(operation),
        payload: JSON.stringify(payload),
    });
}

// The consumer half. A subscription is this isolate's own cursor on the
// operation: the ring is a broadcast, so two isolates subscribed to one
// operation both receive every invocation, and claiming is theirs.
function subscribe(operation) {
    return core.ops.op_claviron_orbit_invoke_subscribe({ operation: normalizedOperation(operation) });
}

function unsubscribe(operation) {
    return core.ops.op_claviron_orbit_invoke_unsubscribe({ operation: normalizedOperation(operation) });
}

// The next invocation under any subscribed operation, as
// `{ id, operation, payload, submittedAt }`, with every cell in the payload
// already a handle. Zero waits; a positive timeout
// yields null when it runs out, and so does falling behind the ring, which
// both mean "ask again".
async function receive(timeoutMs = 0) {
    const raw = await core.ops.op_claviron_orbit_invoke_receive(timeoutMs);
    return raw === null ? null : JSON.parse(raw, revive);
}

let serving = null;

// Subscribe and hand every invocation under `operation` to `handler(payload,
// invocation)`. The returned function stops serving that operation.
//
// One pump serves every operation this isolate handles, and its pending
// promise is what keeps a `js:` task alive, the way `listen()` does for
// events: no timer, no sleep.
function serve(operation, handler) {
    operation = normalizedOperation(operation);
    if (typeof handler !== "function") {
        throw new TypeError("Orbit invocation handler must be a function");
    }
    handlers.set(operation, handler);
    subscribe(operation);
    if (serving === null) {
        serving = (async () => {
            for (;;) {
                let invocation;
                try {
                    invocation = await receive(0);
                } catch {
                    // Nothing subscribed any more, or the transport is gone.
                    serving = null;
                    return;
                }
                if (invocation === null) {
                    continue;
                }
                const handle = handlers.get(invocation.operation);
                if (handle !== undefined) {
                    await handle(invocation.payload, invocation);
                }
            }
        })();
    }
    return () => {
        handlers.delete(operation);
        unsubscribe(operation);
    };
}

Object.defineProperty(globalThis, Symbol.for("claviron.orbit.invoke"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: Object.freeze({ receive, send, serve, subscribe, unsubscribe }),
});
