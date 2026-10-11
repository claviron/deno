const core = globalThis.Deno.core;

function encodeParameter(value) {
    if (value === null || value === undefined) {
        return { type: "null" };
    }
    if (typeof value === "boolean") {
        return { type: "boolean", value };
    }
    if (typeof value === "bigint") {
        return { type: "i64", value: value.toString() };
    }
    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new TypeError("database parameters must contain finite numbers");
        }
        if (Number.isSafeInteger(value)) {
            return value >= -2147483648 && value <= 2147483647
                ? { type: "i32", value }
                : { type: "i64", value: value.toString() };
        }
        return { type: "f64", value };
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
    if (typeof value === "object") {
        return { type: "json", value };
    }
    throw new TypeError(`unsupported database parameter: ${typeof value}`);
}

function decodeValue(value) {
    switch (value.type) {
        case "null":
            return null;
        case "boolean":
        case "i32":
        case "f64":
        case "string":
        case "json":
            return value.value;
        case "i64":
        case "u64":
            return value.value;
        case "bytes":
            return Uint8Array.from(value.value);
        default:
            throw new TypeError(`unsupported database result type: ${value.type}`);
    }
}

async function execute(options) {
    if (options === null || typeof options !== "object") {
        throw new TypeError("database execute options must be an object");
    }
    const result = await core.ops.op_claviron_database_execute({
        backend: String(options.backend),
        connection: String(options.connection ?? "default"),
        sql: String(options.sql),
        parameters: Array.from(options.parameters ?? [], encodeParameter),
        transaction: options.transaction === undefined ? null : String(options.transaction),
    });
    return decodeResult(result);
}

function decodeResult(result) {
    return {
        columns: result.columns,
        rows: result.rows.map((row) => row.map(decodeValue)),
        numAffectedRows: BigInt(result.numAffectedRows),
        insertId: result.insertId === null ? undefined : BigInt(result.insertId),
    };
}

async function begin(options) {
    if (options === null || typeof options !== "object") {
        throw new TypeError("database begin options must be an object");
    }
    const result = await core.ops.op_claviron_database_begin({
        backend: String(options.backend),
        connection: String(options.connection ?? "default"),
        settings: {
            isolationLevel: options.settings?.isolationLevel ?? null,
            accessMode: options.settings?.accessMode ?? null,
        },
    });
    return result.transaction;
}

async function commit(transaction) {
    await core.ops.op_claviron_database_commit({ transaction: String(transaction) });
}

async function rollback(transaction) {
    await core.ops.op_claviron_database_rollback({ transaction: String(transaction) });
}

async function* stream(options) {
    if (options === null || typeof options !== "object") {
        throw new TypeError("database stream options must be an object");
    }
    const batchSize = Number(options.batchSize ?? 100);
    if (!Number.isSafeInteger(batchSize) || batchSize <= 0) {
        throw new TypeError("database stream batchSize must be a positive safe integer");
    }
    const opened = await core.ops.op_claviron_database_stream_open({
        backend: String(options.backend),
        connection: String(options.connection ?? "default"),
        sql: String(options.sql),
        parameters: Array.from(options.parameters ?? [], encodeParameter),
        transaction: options.transaction === undefined ? null : String(options.transaction),
        batchSize,
    });
    try {
        while (true) {
            const batch = await core.ops.op_claviron_database_stream_next({ stream: opened.stream });
            if (batch.done) {
                return;
            }
            yield decodeResult(batch.result);
        }
    } finally {
        await core.ops.op_claviron_database_stream_close({ stream: opened.stream });
    }
}

const database = Object.freeze({ begin, commit, execute, rollback, stream });
Object.defineProperty(globalThis, Symbol.for("claviron.database"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: database,
});
