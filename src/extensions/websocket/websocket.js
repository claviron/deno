import { core } from "ext:core/mod.js";

const {
    op_claviron_websocket_event,
    op_claviron_websocket_send_text,
    op_claviron_websocket_send_binary,
    op_claviron_websocket_close,
} = core.ops;

const bridge = Object.freeze({
    close(id, code, reason) {
        return op_claviron_websocket_close(id, code, reason);
    },

    sendBinary(id, data) {
        return op_claviron_websocket_send_binary(id, data);
    },

    sendText(id, data) {
        return op_claviron_websocket_send_text(id, data);
    },

    takeEvent() {
        return op_claviron_websocket_event();
    },
});

Object.defineProperty(globalThis, Symbol.for("claviron.websocket"), {
    configurable: false,
    enumerable: false,
    writable: false,
    value: bridge,
});
