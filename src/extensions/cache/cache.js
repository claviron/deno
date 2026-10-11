const core = globalThis.Deno.core;

function encodeInteger(value) {
    const text = value.toString();
    return value < 0 ? { type: "i64", value: text } : { type: "u64", value: text };
}

function encodeValue(value, seen = new WeakSet()) {
    if (value === null) {
        return { type: "null" };
    }
    if (typeof value === "boolean") {
        return { type: "boolean", value };
    }
    if (typeof value === "bigint") {
        return encodeInteger(value);
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new TypeError("cache values must contain finite numbers");
        }
        return Number.isSafeInteger(value)
            ? encodeInteger(BigInt(value))
            : { type: "f64", value };
    }
    if (typeof value === "string") {
        return { type: "string", value };
    }
    if (value instanceof Uint8Array) {
        return { type: "bytes", value: Array.from(value) };
    }
    if (ArrayBuffer.isView(value)) {
        return {
            type: "bytes",
            value: Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)),
        };
    }
    if (value instanceof ArrayBuffer) {
        return { type: "bytes", value: Array.from(new Uint8Array(value)) };
    }
    if (typeof value !== "object") {
        throw new TypeError(`unsupported cache value: ${typeof value}`);
    }
    if (seen.has(value)) {
        throw new TypeError("cache values must not contain cycles");
    }
    seen.add(value);
    try {
        if (Array.isArray(value)) {
            return { type: "array", value: value.map((item) => encodeValue(item, seen)) };
        }
        const prototype = Object.getPrototypeOf(value);
        if (prototype !== Object.prototype && prototype !== null) {
            throw new TypeError("cache objects must be plain objects");
        }
        return {
            type: "object",
            value: Object.entries(value,).map(([key, item]) => [key, encodeValue(item, seen)]),
        };
    } finally {
        seen.delete(value);
    }
}

function decodeInteger(value) {
    const integer = BigInt(value);
    return integer >= BigInt(Number.MIN_SAFE_INTEGER) && integer <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(integer)
        : integer;
}

function decodeValue(value) {
    switch (value.type) {
        case "null":
            return null;
        case "boolean":
        case "f64":
        case "string":
            return value.value;
        case "i64":
        case "u64":
            return decodeInteger(value.value);
        case "bytes":
            return Uint8Array.from(value.value);
        case "array":
            return value.value.map(decodeValue);
        case "object":
            return Object.fromEntries(value.value.map(([key, item]) => [key, decodeValue(item)]));
        default:
            throw new TypeError(`unsupported cache result type: ${value.type}`);
    }
}

function ttlSeconds(value) {
    if (value === undefined || value === null) {
        return null;
    }
    if (typeof value === "number" && !Number.isSafeInteger(value)) {
        throw new TypeError("cache TTL must be a safe integer");
    }
    if (typeof value !== "number" && typeof value !== "bigint") {
        throw new TypeError("cache TTL must be an integer");
    }
    return value.toString();
}

function store(name = undefined) {
    const handle = core.ops.op_claviron_cache_store({
        name: name === undefined || name === null ? null : String(name),
    });
    return Object.freeze({
        async get(key) {
            const value = await core.ops.op_claviron_cache_get({ handle, key: String(key) });
            return value === null ? null : decodeValue(value);
        },
        async put(key, value, seconds = undefined) {
            await core.ops.op_claviron_cache_put({
                handle,
                key: String(key),
                value: encodeValue(value),
                ttlSeconds: ttlSeconds(seconds),
            });
        },
        async forget(key) {
            await core.ops.op_claviron_cache_forget({ handle, key: String(key) });
        },
        async clear() {
            await core.ops.op_claviron_cache_clear(handle);
        },
    });
}

const cache = Object.freeze({ store });
Object.defineProperty(globalThis, Symbol.for("claviron.cache"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: cache,
});
