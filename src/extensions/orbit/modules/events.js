const events = globalThis[Symbol.for("claviron.orbit.events")];
export const nodeId = events.nodeId;
export const publish = events.publish;
export const listen = events.listen;
export const subscribe = events.subscribe;
export const subscribeMany = events.subscribeMany;
export const waitFor = events.waitFor;
