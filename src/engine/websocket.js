const websocket = application?.websocket;
const websocketAvailable = websocket !== null
    && typeof websocket === "object"
    && [websocket.open, websocket.message, websocket.close]
        .some((handler) => typeof handler === "function");
const websocketBridge = globalThis[Symbol.for("claviron.websocket")];
const sockets = new Map();

function createSocket(raw) {
    const state = { readyState: 1 };
    const socket = Object.freeze({
        get id() {
            return raw.id;
        },

        get readyState() {
            return state.readyState;
        },

        get url() {
            return raw.url;
        },

        headers: new Headers(raw.headers),

        close(code = 1000, reason = "") {
            if (state.readyState >= 2) {
                return;
            }
            state.readyState = 2;
            websocketBridge.close(raw.id, code, String(reason));
        },

        send(data) {
            if (state.readyState !== 1) {
                throw new Error("WebSocket is not open");
            }
            if (typeof data === "string") {
                websocketBridge.sendText(raw.id, data);
                return;
            }
            if (data instanceof ArrayBuffer) {
                websocketBridge.sendBinary(raw.id, new Uint8Array(data));
                return;
            }
            if (ArrayBuffer.isView(data)) {
                websocketBridge.sendBinary(
                    raw.id,
                    new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
                );
                return;
            }
            throw new TypeError("WebSocket data must be a string, ArrayBuffer, or ArrayBufferView");
        },

        [Symbol.for("claviron.websocket.state")]: state,
    });
    return socket;
}

Object.defineProperty(
    globalThis,
    Symbol.for("claviron.application.websocket.available"),
    {
        configurable: false,
        enumerable: false,
        writable: false,
        value: websocketAvailable,
    },
);

globalThis[Symbol.for("claviron.application.websocket.dispatch")] = async () => {
    if (!websocketAvailable) {
        throw new TypeError("JavaScript application does not export websocket handlers");
    }

    const event = JSON.parse(websocketBridge.takeEvent());
    if (event.type === "open") {
        const socket = createSocket(event);
        sockets.set(event.id, socket);
        await websocket.open?.call(websocket, socket);
        return;
    }

    const socket = sockets.get(event.id);
    if (socket === undefined) {
        throw new Error(`Unknown WebSocket session: ${event.id}`);
    }

    if (event.type === "message") {
        const data = event.message.type === "text"
            ? event.message.data
            : Uint8Array.from(event.message.data);
        await websocket.message?.call(websocket, socket, data);
        return;
    }

    if (event.type === "close") {
        socket[Symbol.for("claviron.websocket.state")].readyState = 3;
        try {
            await websocket.close?.call(
                websocket,
                socket,
                event.code,
                event.reason,
                event.was_clean,
            );
        } finally {
            sockets.delete(event.id);
        }
        return;
    }

    throw new TypeError(`Unknown WebSocket event: ${event.type}`);
};
