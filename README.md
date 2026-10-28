# claviron-deno

Embed persistent JavaScript and TypeScript applications in a
[Claviron](https://crates.io/crates/claviron) host, using Deno Core and V8.
Applications export a Fetch-style handler; Rust owns the isolate pool,
request dispatch and application lifecycle.

Select this crate alongside Claviron in your application. Claviron does not
select or embed the JavaScript engine automatically.

## Application modules

An application exports a function, or an object with a `fetch` method:

```javascript
let requests = 0;

export default {
  fetch(request) {
    return Response.json({
      path: new URL(request.url).pathname,
      requests: ++requests,
    });
  },
};
```

Modules load once per isolate generation. Module state persists across requests;
each configured instance has its own state. Each isolate stays on one OS thread
and advances multiple asynchronous requests through one event loop. A handler
waiting for I/O or response backpressure does not reserve the isolate. Additional
instances provide independent module state and CPU parallelism; they are not
required for concurrent HTTP requests.

Request state, cancellation and response completion belong to each request.
Static responses cross the bridge as complete bytes; live bodies use a bounded
channel created only for that response. Buffering and delivery follow Claviron's
shared HTTP policy. GET/HEAD dispatch never reads or inspects a request body;
other bodyless methods use the transport's existing end-of-stream metadata.

`max_waiting` bounds accepted requests, including unfinished response producers.
`execution_timeout` bounds the wait from acceptance to response headers. Expiry
signals that request's `AbortSignal` and cancels its bridge I/O; it does not
terminate the shared isolate. Synchronous JavaScript still runs on one thread:
a handler that never yields can block that isolate, and cooperative cancellation
cannot preempt it.

The explicit `http-concurrency` example exercises overlapping request bodies,
open response streams, disconnects, producer errors and graceful drain without
binding a network listener or creating fleet resources:

```sh
cargo run --example http-concurrency --all-features
```

Local JavaScript, JSX, TypeScript and TSX modules are supported. TypeScript is
transpiled during loading; type-check it separately in your application's build.
ESM packages resolve from local `node_modules`. The loader does not install
packages, execute CommonJS or provide Node built-ins.

## Register with a host

```toml
[dependencies]
claviron = { version = "0.2.0-alpha.23", default-features = false, features = ["edge"] }
claviron-deno = { version = "0.2.0-alpha.23", features = ["edge"] }
```

In the host's registration callback:

```rust
use claviron::runtime::{AppState, registry};

fn configure(state: &AppState) -> registry::Result<()> {
    claviron_deno::register(state)
}
```

Declare named applications in `js.yaml` under the host's configuration directory:

```yaml
applications:
  api:
    module: app/main.ts
    instances: 2
    env:
      GREETING: Hello
```

Relative module paths resolve from the process's working directory. Application
environment values are explicit; the JavaScript environment is not an implicit
view of the host process's environment. `env_file` can supply additional values.

With the `edge` feature, a vhost route selects the named application:

```yaml
example.test:
  routes:
    /api:
      js:
        application: api
```

The route mounts an application; it does not create another isolate pool.
For a host without vhosts, select `server` and compose the public
`routing::build_application_router` or `routing::build_router` functions with
your Axum router instead.

## Features

Default features are empty. The base crate exposes `ApplicationDefinition` and
the thread-local `CoreApplication` for hosts that manage execution themselves.

| Feature | Adds |
| --- | --- |
| `server` | Managed application pools, `js.yaml`, lifecycle registration and Axum mounting. |
| `edge` | `server` plus Claviron's `js:` vhost route extension. |
| `websocket` | `server` plus inbound WebSocket callbacks over HTTP/1 Upgrade and HTTP/2 extended CONNECT. |
| `orbit` | `server` plus fleet events, shared cells, counters, locks and invocation modules. |
| `cache` | `server` plus access to Claviron cache stores. |
| `postgres` | `server` plus the PostgreSQL database bridge. |
| `mysql` | Adds MySQL to the database bridge; currently also selects `postgres`. |
| `tasks` | `server` plus a JavaScript executor for Claviron tasks. |
| `log` | `server` plus declared log channels and a LogTape sink. |
| `fetch` | Deno's outbound Fetch implementation and its supporting network stack. |
| `tls` | `server` and `fetch`, with access to the host's configured certificate trust store. |

## Native JavaScript modules

Claviron capabilities are imported through public `claviron:*` ESM modules:

| Module | Capability |
| --- | --- |
| `claviron:runtime` | Application identity, declared environment and cooperative cancellation. |
| `claviron:orbit/events` | Application-scoped publication, subscriptions and one-shot waits. |
| `claviron:orbit/cell` | Typed fleet-shared values. |
| `claviron:orbit/counter` | Shared counters. |
| `claviron:orbit/lock` | Shared locks. |
| `claviron:orbit/invoke` | Native invocation bridge. |
| `claviron:cache` | Cache store access. |
| `claviron:db` | Bound queries, transactions and result streaming over Rust-owned database connections. |
| `claviron:log` | Declared logging channels. |
| `claviron:log/logtape` | LogTape adapter. |

These modules use the corresponding Cargo features and host services. For
example, `orbit` enables the Orbit modules; the host still supplies its fleet.
Applications do not call private Deno ops directly.

## Lifecycle and WebSockets

An exported application object may provide `shutdown()` for cleanup. The
`claviron:runtime` cancellation signal is delivered when the application begins
stopping, before that cleanup hook runs. Accepted work drains according to the
configured engine or application `graceful_timeout`.

With `websocket`, the object can also implement `websocket.open`,
`websocket.message` and `websocket.close`. Each connection stays attached to one
application instance; network I/O remains outside its V8 thread. Edge hosts must
also enable the relevant HTTP transport policy and declare the WebSocket route.

## Runtime scope

This is an embedded Deno Core runtime with selected Web APIs, not the full Deno
CLI environment. Your host owns listeners, TLS, routing, configuration and
service registration. JavaScript applications remain inside that host process.

For a separate Node, Deno or Bun process connected to a Claviron edge, use
[@claviron/orbit](https://github.com/claviron/orbit-js), backed by
[Orbit Link](https://crates.io/crates/orbitive).

APIs and configuration may change between alpha releases.

## License

MIT OR Apache-2.0; see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
