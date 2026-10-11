const core = globalThis.Deno.core;

function amount(value) {
    if (typeof value === "number" && !Number.isSafeInteger(value)) {
        throw new TypeError("Orbit counter amount must be a safe integer or bigint");
    }
    if (typeof value !== "number" && typeof value !== "bigint") {
        throw new TypeError("Orbit counter amount must be an integer");
    }
    return value.toString();
}

function get(key) {
    const value = core.ops.op_claviron_orbit_counter_get({ key: String(key) });
    return value === null ? null : BigInt(value);
}

function increment(key, by = 1n) {
    return BigInt(core.ops.op_claviron_orbit_counter_increment({
        key: String(key),
        amount: amount(by),
    }));
}

function decrement(key, by = 1n) {
    return BigInt(core.ops.op_claviron_orbit_counter_decrement({
        key: String(key),
        amount: amount(by),
    }));
}

function reset(key) {
    core.ops.op_claviron_orbit_counter_reset({ key: String(key) });
}

const counter = Object.freeze({ decrement, get, increment, reset });
Object.defineProperty(globalThis, Symbol.for("claviron.orbit.counter"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: counter,
});
