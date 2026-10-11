const core = globalThis.Deno.core;

const LEVELS = ["trace", "debug", "info", "warn", "error"];

function isPlainObject(value) {
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
}

function text(value) {
    try {
        return JSON.stringify(value) ?? String(value);
    } catch {
        return String(value);
    }
}

function pushError(name, error, out) {
    out.push([`${name}.class`, { type: "string", value: error.constructor?.name ?? error.name }]);
    out.push([`${name}.message`, { type: "string", value: String(error.message) }]);
    if (typeof error.stack === "string") {
        out.push([`${name}.trace`, { type: "string", value: error.stack }]);
    }
}

// Nested plain objects become dotted keys; an Error becomes `key.class`,
// `key.message` and `key.trace`, the fields PHP and Python records carry.
function pushField(name, value, out, seen) {
    if (value === undefined) {
        return;
    }
    if (value === null) {
        out.push([name, { type: "string", value: "" }]);
    } else if (typeof value === "boolean") {
        out.push([name, { type: "bool", value }]);
    } else if (typeof value === "number") {
        if (Number.isSafeInteger(value)) {
            out.push([name, { type: "i64", value }]);
        } else if (Number.isFinite(value)) {
            out.push([name, { type: "f64", value }]);
        } else {
            out.push([name, { type: "string", value: String(value) }]);
        }
    } else if (typeof value === "string") {
        out.push([name, { type: "string", value }]);
    } else if (value instanceof Error) {
        pushError(name, value, out);
    } else if (typeof value === "object" && !Array.isArray(value) && isPlainObject(value)) {
        if (seen.has(value)) {
            return;
        }
        seen.add(value);
        for (const [key, item] of Object.entries(value)) {
            pushField(`${name}.${key}`, item, out, seen);
        }
        seen.delete(value);
    } else if (typeof value === "object") {
        out.push([name, { type: "string", value: text(value) }]);
    } else {
        out.push([name, { type: "string", value: String(value) }]);
    }
}

function fields(context) {
    const out = [];
    if (context === undefined || context === null) {
        return out;
    }
    const seen = new WeakSet([context]);
    for (const [key, value] of Object.entries(context)) {
        pushField(key, value, out, seen);
    }
    return out;
}

function resolve(name) {
    const id = core.ops.op_claviron_log_channel(String(name));
    if (id < 0) {
        throw new Error(`Claviron log channel "${name}" is not declared in logging.yaml`);
    }
    return id;
}

// One record to a resolved channel. `message` may be an Error: its message is
// the record's, the error itself the `exception` fields.
function write(id, level, message, context, target) {
    const out = fields(context);
    if (message instanceof Error) {
        pushError("exception", message, out);
        message = message.message;
    }
    return core.ops.op_claviron_log({
        channel: id,
        level: String(level),
        target: target === undefined || target === null ? null : String(target),
        message: String(message),
        fields: out,
    });
}

function channel(name, options = undefined) {
    const id = resolve(name);
    const target = options?.target;
    const logger = {};
    for (const level of LEVELS) {
        logger[level] = (message, context = undefined) =>
            write(id, level, message, context, target);
    }
    return Object.freeze(logger);
}

const log = Object.freeze({ channel, resolve, write });
Object.defineProperty(globalThis, Symbol.for("claviron.log"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: log,
});
