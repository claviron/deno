const core = globalThis.Deno.core;
const { revive } = globalThis[Symbol.for("claviron.orbit.cell")];
const listeners = new Map();

function normalizedTopic(topic) {
    const value = String(topic).trim();
    if (value.length === 0) {
        throw new TypeError("Orbit event topic cannot be empty");
    }
    return value;
}

function publish(topic, payload = null) {
    core.ops.op_claviron_orbit_event_publish({
        topic: normalizedTopic(topic),
        payload: JSON.stringify(payload),
    });
}

function nodeId() {
    return core.ops.op_claviron_orbit_event_node_id();
}

function subscribe(topic, listener) {
    topic = normalizedTopic(topic);
    if (typeof listener !== "function") {
        throw new TypeError("Orbit event listener must be a function");
    }
    let topicListeners = listeners.get(topic);
    if (topicListeners === undefined) {
        topicListeners = new Set();
        listeners.set(topic, topicListeners);
    }
    topicListeners.add(listener);
    return () => {
        topicListeners.delete(listener);
        if (topicListeners.size === 0) {
            listeners.delete(topic);
        }
    };
}

function subscribeMany(topics, listener) {
    if (!Array.isArray(topics)) {
        throw new TypeError("Orbit event topics must be an array");
    }
    if (typeof listener !== "function") {
        throw new TypeError("Orbit event listener must be a function");
    }
    const normalized = [...new Set(topics.map(normalizedTopic))];
    const unsubscribe = normalized.map((topic) => subscribe(topic, listener));
    return () => {
        for (const remove of unsubscribe) {
            remove();
        }
    };
}

async function waitFor(topic, timeoutMs = 0) {
    topic = normalizedTopic(topic);
    timeoutMs = Number(timeoutMs);
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 0xffffffff) {
        throw new TypeError("Orbit event timeout must be an unsigned 32-bit integer");
    }

    // Opening the cursor is synchronous and deliberately precedes the first
    // await. A caller can create this promise, trigger external work, and know
    // an event published immediately by that work cannot race the subscription.
    const handle = core.ops.op_claviron_orbit_event_wait_begin();
    const event = await core.ops.op_claviron_orbit_event_wait({handle, topic, timeoutMs});
    return event === null ? null : JSON.parse(event, revive);
}

async function deliver(event) {
    const topicListeners = listeners.get(event.topic);
    if (topicListeners === undefined) {
        return;
    }
    const payload = event.payload;
    for (const listener of [...topicListeners]) {
        await listener(payload, event.topic);
    }
}

// An application does not pull: the host hands each running isolate the event
// and calls this. Topic filtering still happens here, so an application that
// subscribed to nothing discards it.
async function dispatch() {
    await deliver(JSON.parse(core.ops.op_claviron_orbit_event_take(), revive));
}

let listening = null;

// A task has no host loop handing it anything, so it pulls its own.
//
// The returned promise never settles while the bus is open, and that is the
// point twice over: it is how the task reacts to events, and it is what keeps
// the event loop alive. A `js:` daemon therefore needs no timer to stay up -
// leaving this pending is the JavaScript spelling of `while (!cancelled)`.
//
// Idempotent: the second call joins the first pump rather than opening a
// second one, so two modules can both ask to listen.
function listen() {
    if (listening !== null) {
        return listening;
    }
    listening = (async () => {
        for (;;) {
            let event;
            try {
                // Zero: wait, do not poll. There is nothing to check for.
                event = await core.ops.op_claviron_orbit_event_receive(0);
            } catch {
                // The bus is closed; there will never be another event.
                listening = null;
                return;
            }
            // null means the isolate fell behind and the channel dropped what
            // it missed. Nothing to deliver, so ask again.
            if (event !== null) {
                await deliver(JSON.parse(event, revive));
            }
        }
    })();
    return listening;
}

Object.defineProperty(globalThis, Symbol.for("claviron.orbit.events"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: Object.freeze({ dispatch, listen, nodeId, publish, subscribe, subscribeMany, waitFor }),
});
