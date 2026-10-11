import { isCancelled } from "claviron:runtime";

let activeSocket;
export default {
    fetch() { return new Response("ready"); },
    shutdown() {
        activeSocket.send(`cancelled:${isCancelled()}`);
    },
    websocket: {
        open(socket) {
            activeSocket = socket;
            socket.send("open");
            setTimeout(() => socket.send("timer"), 10);
        },
        message() {},
        close() {},
    },
};
