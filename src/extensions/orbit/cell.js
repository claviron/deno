const core = globalThis.Deno.core;

// Construction goes through the static factories; the token keeps `new`
// from producing a handle that points at nothing.
const TOKEN = Symbol("claviron.orbit.cell");
const KINDS = new Set(["int", "float", "bool", "string"]);

function decode(kind, value) {
    switch (kind) {
        case "int":
            return BigInt(value);
        case "float":
            return Number(value);
        case "bool":
            return value === "true";
        default:
            return value;
    }
}

function encode(kind, value) {
    switch (kind) {
        case "int":
            if (typeof value === "bigint") {
                return value.toString();
            }
            if (typeof value === "number" && Number.isSafeInteger(value)) {
                return value.toString();
            }
            throw new TypeError("an int cell takes a safe integer or a bigint");
        case "float":
            if (typeof value === "number" && Number.isFinite(value)) {
                return value.toString();
            }
            if (typeof value === "bigint") {
                return value.toString();
            }
            throw new TypeError("a float cell takes a finite number");
        case "bool":
            if (typeof value === "boolean") {
                return value ? "true" : "false";
            }
            throw new TypeError("a bool cell takes a boolean");
        default:
            if (typeof value === "string") {
                return value;
            }
            throw new TypeError("a string cell takes a string");
    }
}

function decodeRevision(kind, value) {
    return kind === "string" ? BigInt(value) : Number(value);
}

function encodeRevision(kind, value) {
    if (kind === "string") {
        if (typeof value !== "bigint") {
            throw new TypeError("a string cell revision must be a bigint");
        }
        if (value < 0n || value > 0xffffffffffffffffn) {
            throw new RangeError("a string cell revision must fit an unsigned 64-bit integer");
        }
    } else if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > 0xffffffff) {
        throw new RangeError("a scalar revision must fit an unsigned 32-bit integer");
    }
    return value.toString();
}

// A typed fleet cell as a JavaScript value.
//
// The handle holds only the id; the value lives in Orbit shared memory and
// every process that opens the same id reads and updates the same cell.
// JavaScript has no operator overloading, so `n.add(1)` is the spelling of
// PHP's `$n++`; where an operator wants a primitive, `valueOf()` reads the
// cell, so `n > 3` and `${s}` work and yield plain values, not handles.
// `JSON.stringify(cell)` is the id, the way PHP's `serialize()` is: a cell
// crosses an event or an invocation by reference, and `Orbital.open(id)` on
// the other side is the same cell.
class Orbital {
    #id;
    #kind;

    constructor(id, kind, token) {
        if (token !== TOKEN) {
            throw new TypeError("use Orbital.int(), .float(), .bool(), .string() or .open()");
        }
        this.#id = id;
        this.#kind = kind;
    }

    static #allocate(kind, value) {
        const id = core.ops.op_claviron_orbit_cell_allocate({ kind, value: encode(kind, value) });
        return new Orbital(id, kind, TOKEN);
    }

    static int(value = 0n) {
        return Orbital.#allocate("int", value);
    }

    static float(value = 0) {
        return Orbital.#allocate("float", value);
    }

    static bool(value = false) {
        return Orbital.#allocate("bool", value);
    }

    static string(value = "") {
        return Orbital.#allocate("string", value);
    }

    // Reattach to a cell by id; the cell says which type it is.
    static open(id) {
        id = String(id);
        const kind = core.ops.op_claviron_orbit_cell_open({ id });
        if (!KINDS.has(kind)) {
            throw new TypeError(`Orbital: unknown cell kind ${kind}`);
        }
        return new Orbital(id, kind, TOKEN);
    }

    get id() {
        return this.#id;
    }

    get kind() {
        return this.#kind;
    }

    get() {
        return decode(this.#kind, core.ops.op_claviron_orbit_cell_get({ id: this.#id }));
    }

    set(value) {
        core.ops.op_claviron_orbit_cell_set({ id: this.#id, value: encode(this.#kind, value) });
        return this;
    }

    // Atomic add on an int or float cell; the new value comes back.
    add(by = 1) {
        return decode(this.#kind, core.ops.op_claviron_orbit_cell_add({ id: this.#id, by: encode(this.#kind, by) }));
    }

    // Take up to `by` while the cell stays at or under `limit`, in one atomic
    // step; `{taken, after}` back, or `null` when nothing is left. `taken`
    // can be less than asked for -- whoever finds a remainder gets it rather
    // than being refused.
    //
    // This is what `add()` cannot do. `add()` lands before the caller can
    // test the result, so N callers racing one limit pass it by up to N-1
    // between them. Here the bound is inside the swap, so the cell never
    // passes `limit` however many are racing.
    addUntil(by, limit) {
        const claim = core.ops.op_claviron_orbit_cell_add_until({
            id: this.#id,
            by: encode(this.#kind, by),
            limit: encode(this.#kind, limit),
        });
        return claim.took
            ? {taken: decode(this.#kind, claim.taken), after: decode(this.#kind, claim.after)}
            : null;
    }

    // The loop a claim is for. `for (const {taken, after} of cell.upTo(n))`
    // writes no stop condition and cannot overshoot; it ends when the cell
    // reaches `n`, whoever got there. `chunk` is how much to ask for at a
    // time: raising it touches the contended word that many times less
    // often, and the count stays exact either way.
    *upTo(total, {chunk = 1} = {}) {
        for (;;) {
            const claim = this.addUntil(chunk, total);
            if (claim === null) {
                return;
            }
            yield claim;
        }
    }

    // Atomic append on a string cell; the whole text comes back. A value that
    // does not fit is refused, nothing is written.
    append(text) {
        return core.ops.op_claviron_orbit_cell_append({ id: this.#id, text: String(text) });
    }

    // Scalar write counts are Numbers; string revisions are full-width BigInts.
    // Take the token before `get()` and no write between the two is missed.
    version() {
        return decodeRevision(this.#kind, core.ops.op_claviron_orbit_cell_version(this.#id));
    }

    // Resolves with the new write count once the cell has been written since
    // `since`, by any process; the isolate keeps running meanwhile. Ten
    // writes while waiting resolve it once, and `get()` reads the latest.
    // Rejects when the cell has been released.
    changed(since) {
        return core.ops.op_claviron_orbit_cell_wait_changed({
            id: this.#id,
            since: encodeRevision(this.#kind, since),
        }).then(value => decodeRevision(this.#kind, value));
    }

    // Retires the cell for every handle, in every process.
    release() {
        core.ops.op_claviron_orbit_cell_release({ id: this.#id });
    }

    valueOf() {
        return this.get();
    }

    toString() {
        return String(this.get());
    }

    // The wire form, `{"@orbital": id, "kind": kind}`: what PHP's
    // `serialize()` is for its handle. A payload carrying a cell crosses an
    // event or an invocation by reference, and `revive` on the other side
    // hands back a handle without touching shared memory.
    toJSON() {
        return { [MARKER]: this.#id, kind: this.#kind };
    }
}

const MARKER = "@orbital";

// A `JSON.parse` reviver: every `{"@orbital": id}` becomes a handle. The
// kind is taken from the marker when it is there; a marker without one (a
// writer that only knows the id) is opened, which asks the cell.
function revive(_key, value) {
    if (value === null || typeof value !== "object" || typeof value[MARKER] !== "string") {
        return value;
    }
    const kind = value.kind;
    return KINDS.has(kind) ? new Orbital(value[MARKER], kind, TOKEN) : Orbital.open(value[MARKER]);
}

Object.freeze(Orbital);
Object.defineProperty(globalThis, Symbol.for("claviron.orbit.cell"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: Object.freeze({ Orbital, revive }),
});
