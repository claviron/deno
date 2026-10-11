const core = globalThis.Deno.core;

function ttlMilliseconds(value) {
    if (typeof value === "number" && (!Number.isSafeInteger(value) || value <= 0)) {
        throw new TypeError("Orbit lock TTL must be a positive safe integer or bigint");
    }
    if (typeof value === "bigint" && value <= 0n) {
        throw new TypeError("Orbit lock TTL must be positive");
    }
    if (typeof value !== "number" && typeof value !== "bigint") {
        throw new TypeError("Orbit lock TTL must be an integer");
    }
    return value.toString();
}

function tryAcquire(key, owner, ttlMs) {
    return core.ops.op_claviron_orbit_lock_try_acquire({
        key: String(key),
        owner: String(owner),
        ttlMs: ttlMilliseconds(ttlMs),
    });
}

function release(key, owner) {
    return core.ops.op_claviron_orbit_lock_release({
        key: String(key),
        owner: String(owner),
    });
}

function owner(key) {
    return core.ops.op_claviron_orbit_lock_owner({ key: String(key) });
}

function forceRelease(key) {
    return core.ops.op_claviron_orbit_lock_force_release({ key: String(key) });
}

const lock = Object.freeze({ forceRelease, owner, release, tryAcquire });
Object.defineProperty(globalThis, Symbol.for("claviron.orbit.lock"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: lock,
});
