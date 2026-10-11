import { core } from "ext:core/mod.js";

Object.defineProperty(globalThis, Symbol.for("claviron.test"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: Object.freeze({
        value() {
            return core.ops.op_claviron_test_value();
        },
    }),
});
