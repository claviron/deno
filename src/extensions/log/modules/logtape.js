// A LogTape sink on a Claviron log channel, the way `claviron/monolog` is a
// Monolog handler on one. LogTape is not imported: a sink is a function of a
// log record, so this module carries no dependency on it.
//
//     import { configure, getLogger } from "@logtape/logtape";
//     import { getClavironSink } from "claviron:log/logtape";
//
//     await configure({
//         sinks: { claviron: getClavironSink("app") },
//         loggers: [{ category: "app", sinks: ["claviron"] }],
//     });
//
// The category is the record's target; properties become fields.
const log = globalThis[Symbol.for("claviron.log")];

function render(parts) {
    let message = "";
    for (let index = 0; index < parts.length; index++) {
        const part = parts[index];
        if (index % 2 === 0) {
            message += part;
        } else if (typeof part === "string") {
            message += part;
        } else {
            try {
                message += JSON.stringify(part) ?? String(part);
            } catch {
                message += String(part);
            }
        }
    }
    return message;
}

export function getClavironSink(channel) {
    const id = log.resolve(channel);
    return (record) => {
        log.write(
            id,
            record.level,
            render(record.message),
            record.properties,
            record.category.join("."),
        );
    };
}
