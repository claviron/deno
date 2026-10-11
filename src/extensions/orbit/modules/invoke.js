const invoke = globalThis[Symbol.for("claviron.orbit.invoke")];
export const receive = invoke.receive;
export const send = invoke.send;
export const serve = invoke.serve;
export const subscribe = invoke.subscribe;
export const unsubscribe = invoke.unsubscribe;
