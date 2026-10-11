const counter = globalThis[Symbol.for("claviron.orbit.counter")];
export const decrement = counter.decrement;
export const get = counter.get;
export const increment = counter.increment;
export const reset = counter.reset;
