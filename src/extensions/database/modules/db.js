const database = globalThis[Symbol.for("claviron.database")];
export const begin = database.begin;
export const commit = database.commit;
export const execute = database.execute;
export const rollback = database.rollback;
export const stream = database.stream;
