const lock = globalThis[Symbol.for("claviron.orbit.lock")];
export const forceRelease = lock.forceRelease;
export const owner = lock.owner;
export const release = lock.release;
export const tryAcquire = lock.tryAcquire;
