# claviron-deno architecture

`claviron-deno` owns a slim embedded JavaScript application runtime.
It embeds `deno_core` directly and deliberately does not provide the complete
Deno CLI/runtime environment.

## Boundaries

- `application/` is the backend-independent application contract and request /
  response body model. Request and response aliases use the standard `http`
  crate, without selecting Claviron's server or Edge features.
- `engine/` owns the slim `deno_core::JsRuntime`, local ESM loading,
  TypeScript transpilation and its Web platform composition. The slim runtime
  registers the compatible `deno_webidl`, `deno_web` and `deno_crypto`
  extensions in dependency order, then exposes their standard URL, event,
  encoding, stream, timer and Web Crypto globals through a Claviron-owned
  bootstrap. Deno and V8 types do not leak into the public application API.
- `extensions/` owns native Claviron ops and their JavaScript ESM wrappers.
  Extensions depend on `ExtensionContext`, not on a concrete engine.
- With the `postgres` feature, `extensions::database` exposes the shared
  `claviron:db` query contract and binds it to the reloadable pools owned by
  `db`. The `mysql` feature adds MySQL binding and decoding without
  changing that JavaScript contract. Transactions are isolate-local opaque
  handles: Rust pins one SQLx pool connection until commit or rollback, while
  JavaScript never owns credentials or a native connection. An unfinished
  transaction is rolled back when its isolate is dropped. Database result
  streaming uses isolate-local opaque stream handles and a capacity-one
  channel: a Rust task owns the SQLx row stream and emits typed row batches
  only as JavaScript consumes them. A transaction slot is lent to that task
  while streaming and restored before commit or rollback can continue.
- With the `log` feature, `extensions::log` exposes `claviron:log` and the
  LogTape sink `claviron:log/logtape` over the channels `logging.yaml`
  declares. A record is copied into the fleet's log ring and the call
  returns; an undeclared channel throws when it is opened, not per record.
- `backend::deno::ExtensionFactory` is the low-level seam for sibling crates.
  A factory contributes fresh native ops and public `claviron:*` ESM modules
  to each isolate while retaining shared Rust services outside V8.
- `server/` is feature-gated. It owns the worker-local Runnable, persistent
  instance pools, typed mailbox, common application-management controller and
  raw HTTP request adapter.
- `routing` uses the same `server` feature and owns Axum mounting, its typed
  JavaScript mount declarations and dispatch. Its builder never needs Edge's
  HostConfig or a JS feature in Claviron.
- The optional `edge` feature adds the existing Edge route-extension binding.
  It is a topology boundary, not another engine: plain Axum consumers do not
  acquire Edge. Claviron and Edge retain no dependency on this Deno crate.
- `backend::deno` is a hidden native extension seam. Deno and V8 types stay
  behind it rather than becoming the downstream application contract.

The JavaScript-facing extension namespace is `claviron:*`. Application code
must not call `Deno.core.ops` directly. The slim engine maps those public
specifier names to private `ext:` modules.

Embedded `js:` tasks use the executor-independent `tasks.yaml` `log:` policy.
`console.log` writes stdout and warning/error levels write stderr; relative
file targets resolve against the task `cwd`. The sink travels in that task
isolate's `ExtensionContext`, so concurrent tasks may select different files
without redirecting process-global descriptors. Application isolates retain
no configurable output policy: their console records enter tracing with
`runtime`, `application`, and `instance` fields. The process logging layer adds
the worker identity and delivers those records to journald under systemd.

## Application contract

An application entry is a persistent ES module whose default export is either
a Fetch-style function or an object with `fetch(request)`. Module boot happens
once per application generation. Each configured instance owns one V8 isolate
on one OS thread. The owner starts calls without awaiting each call to
completion, then polls its command mailbox, the V8 event loop and pending call
completions together. Requests are distributed round-robin through one typed
mailbox per isolate. Their retained application admission permit bounds queued
and executing HTTP work. Non-HTTP commands reserve separate mailbox capacity
until dequeued. Both use the same FIFO, so shutdown remains behind previously
enqueued work. A live `JsRuntime` never crosses the channel. A handler's
asynchronous wait does not reserve an instance. Synchronous JS remains serial.
The per-instance one-request semaphore is removed.

The bridge converts Claviron HTTP requests to Web `Request` values and Web
`Response` values back to Claviron responses. HTTP request/response parts and
their `HeaderMap` move through the mailbox and adapter directly. Dispatch passes
an exchange handle, body kind and a Deno GC-backed object holding the method,
URI, headers and buffered body. With `fetch`, Deno's `fromInnerRequest`
constructs the branded Web Request over an isolate-local inner target. Method,
URL, headers, body and signal are materialized when accessed. Metadata ops read
the typed backing object directly, without looking up the live exchange.
Retained Requests keep their metadata and buffered body readable after a reply;
the backing object owns no admission permit, live stream endpoint, response
sender or transport extensions. Those remain in the separately closed exchange
resource. Cloning and body consumption use Deno's Fetch types.
Native buffered capacity and known metadata storage contribute to V8 external
memory pressure. Moving buffered bytes into an ArrayBuffer removes the native
charge; a weak V8 finalizer releases the remaining charge on collection. This
is payload accounting, not an exact report of opaque HeaderMap/Uri allocator
overhead, and it does not retain a strong handle to the Request.
The inbound-only shim retains its existing eager Request construction.

Ready replies use Deno's existing static Response fields, without a public
Headers iterator or an intermediate TextEncoder buffer. Native ops take an
owned UTF-8 or byte copy before replying across threads; mutable V8 memory is
not shared with the HTTP owner. Other responses use Deno's `wireHeaderList`,
including its decoded-body header handling. Neither direction encodes or parses
a JSON head. Header values preserve Fetch ByteString (Latin-1) bytes, including
repeated fields. Header array access permits guest getters without retaining an
OpState or request-resource borrow across guest execution. This bridge uses
compatible `deno_fetch` internals; it does not import Deno's HTTP transport or
change Claviron's application scheduling.
Each call owns a native request
resource handle containing its request, reply, body endpoints, cancellation and
admission lifetime. No singleton request/reply slots remain in `OpState`.
Handles are exact JavaScript-safe 53-bit integers, never recycled or wrapped.
Exhaustion rejects allocation instead of aliasing a retained handle. Resource mutation stays on the V8
owner thread; a streamed read takes its receiver across the await and rejects
concurrent reads instead of locking it.

GET/HEAD dispatch does not inspect or collect a request body. Other methods use
the transport's end-of-stream metadata before choosing whole or streamed input
through `HttpPolicy`. Static replies use Deno's existing static-body
representation and cross once as bytes, without creating a response channel,
stream reader or pump. Live replies allocate their bounded channel on demand.
`claviron-http::body::deliver_reply` owns delivery/buffering policy; the bridge
has no SSE-specific dispatch or scheduling branch. Failed producers preserve an
error after their queued bytes rather than reporting a successful EOF.

Dropping an HTTP caller or live response cancels only its request. The owner
signals its `AbortSignal`, wakes pending bridge I/O and keeps ownership of guest
cleanup until the call settles. A request body pump also wakes when its consumer
closes, even if its client sends no further frames. Normal static replies need
no cancellation waiter registration when their promise is already complete.
Persistent module state remains intentional, so framework instances, dependency
containers and connection clients may be created once and reused.

The HTTP adapter constructs the absolute Web Request URL from accepted
transport scheme and request authority, preserving port, path and query.
The application-facing request carries that absolute URI with its original
path and query. Connection facts override any
absolute-URI scheme or client forwarding header. A direct caller without
transport context must provide a typed HTTP(S) scheme plus authority, or an
absolute HTTP(S) URI; missing information is an error, never invented
`https://localhost`.

With the `tls` feature, outbound `fetch()` keeps Deno's public trust roots and
also trusts the process `CertAuthority` registered from `tls.yaml`'s certstore.
Application and `js:` task isolates receive the already-loaded DER roots; they
do not reopen a CA path or depend on Deno CLI processing of `DENO_CERT` or
`NODE_EXTRA_CA_CERTS`.

`URL`, `URLSearchParams` and the foundational Web globals come from Deno's
official Web extensions. With `fetch`, Request/Response and static-body access
use the compatible Deno Fetch extension. Without it, the small Claviron bridge
provides inbound Request/Response only, including live bodies; it does not grant
outbound network access.

`CoreApplication` remains `!Send` because its V8 isolate stays on its owner OS
thread. The server feature is the only layer allowed to decide thread and pool
lifecycle.

With the optional `websocket` feature, the same default-exported application
object may also expose `websocket.open(socket)`,
`websocket.message(socket, data)`, and
`websocket.close(socket, code, reason, wasClean)`. Hyper performs the H1
upgrade or H2 extended CONNECT and `fastwebsockets` owns frame I/O on a
listener-owned Tokio task; the V8 isolate
never owns a network stream. A connection is pinned to one application
instance, while open/message/close events enter that instance through its
bounded command mailbox. Consequently, an idle socket does not occupy the
isolate thread and HTTP requests remain part of the same scheduling model.

The application-facing socket supports `send(string | ArrayBufferView |
ArrayBuffer)` and `close(code, reason)`. Outbound actions use a bounded channel
back to the socket task. This is a Claviron transport bridge, not Deno's client
WebSocket implementation, so enabling inbound WebSockets does not pull in
`deno_websocket`, `deno_net`, or a second HTTP/TLS stack.

The application object may expose `shutdown()`. The owner thread invokes it
before dropping the isolate, after all earlier mailbox commands. An application
can therefore send a final WebSocket message and then call `close(1001, reason)`;
the ordered action channel delivers the text frame before the close frame, and
returning from the socket task releases either the H1 upgrade or active H2
CONNECT stream. If the application emits no close action, dropping its action
sender wakes the transport fallback. Process shutdown then uses `1001`; an
unexpected application loss uses `1011`.

Every application pool owns one cancellation token for its current generation.
`claviron:runtime` exposes it as the synchronous `isCancelled()` check and the
awaitable `cancelled()` notification. A named stop or restart and aggregate
service cancellation cancel that token before queuing the shutdown command, so
an active asynchronous request can observe cancellation while other requests continue through the same
isolate event loop. The later `shutdown()` hook is cleanup, not the first
delivery of the signal. A new application generation receives a fresh token.
Embedded JavaScript tasks receive their occurrence token through the same
runtime API.

Neither the application hook nor WebSocket delivery invents a private drain
deadline. The lifecycle owner supplies the explicit shutdown policy; the
supervisor remains the process-level last resort.

With the `orbit` feature, `claviron:orbit/events` exposes application-scoped
`publish(topic, payload)` and `subscribe(topic, listener)` primitives. One
common `ApplicationEvent` envelope carries the `js:<application>` target,
topic, envelope ID, and JSON-shaped payload through the existing typed process
bus and Orbit fleet bridge. Rust drops frames targeted at another runtime or
application before they reach the isolate. A matching event is dispatched to
every isolate in that application's pool; peer workers receive the same event
and dispatch it to their own isolates. Explicit targetless shared topics use
the same ring but remain a separate adapter contract. JavaScript callbacks
always run on the owning V8 thread through the normal bounded application
mailbox. They never execute on the Orbit poller.

The event ring is ephemeral, bounded fan-out rather than durable messaging.
There is no replay or acknowledgement, lag may drop old events, and the Orbit
frame-size limit is validated before a JavaScript publication enters the local
bus. `nodeId()` exposes the current fleet node only for stable application
instance identity; it does not expose the raw Orbit transport.

`waitFor(topic, timeoutMs)` is the request-safe one-shot form. It opens a fresh
event-bus cursor synchronously before returning its promise, then consumes only
the first matching event and drops that cursor. This lets an application open
the observation first, trigger another runtime with `fetch()`, and await the
resulting shared event without depending on the application's serialized host
command mailbox. It has no replay guarantee; timeout or receiver lag returns
`null`.

Callback subscriptions and one-shot waits declare their domain topics through
the process's existing `LinkInterest`. This is the same interest contract as
PHP and standalone Orbit JS: Link forwards only topics with live consumers,
not every application event. The first callback for a topic declares it; the
last unsubscribe withdraws it. Each waiter holds a separate interest guard
until delivery, timeout or cancellation, so it cannot withdraw a callback's
or another isolate's declaration. Isolate teardown releases its declarations.
The task inbox and one-shot receiver filter topic and application target at
publication, before unrelated events can occupy their bounded queues.
No new event loop, capacity, poll or retry duration is introduced.

## TypeScript and packages

The slim loader accepts JavaScript, JSX, TypeScript and TSX local modules.
TypeScript is transpiled at module load with source maps; it is not type-checked
at runtime. Projects should keep a separate `tsc --noEmit` check.

Bare ESM imports resolve through the closest `node_modules` directory using a
package's `exports.import`, `exports.default`, `module`, or `main` entry. This
is deliberately not Node compatibility: CommonJS execution, Node built-ins,
package installation, import maps and remote URL loading are outside the slim
engine. Hono works because its application path uses Web standards and ESM.

## Configuration and routing

Each application may declare `frameworks` in `js.yaml`, or through
`ApplicationDefinition::frameworks` in code. Scalar `framework` and the singular
builder remain single-label shorthands. Explicit lists, including `[]`, win.
The execution owner prepares its
common Application description as JavaScript / Claviron / fetch plus that
framework tags and optional symbolic `icon` (also `ApplicationDefinition::icon`).
An omitted icon resolves to `deno` for this embedded `deno_core` adapter;
explicit codes are preserved and the runtime label remains JavaScript.
The GUI owns icon resolution and defaults; no path or URL is loaded. This is
management metadata only, not module discovery,
routing policy or a new runner. Local and Link reports use the same row.

When no framework is declared, application registration reads the nearest
`package.json` above the entry module. Its `dependencies` and `peerDependencies`
identify Hono, Elysia, Express, Fastify, Koa, NestJS or Hapi; multiple distinct
frameworks remain distinct tags. Development dependencies and framework helper
packages do not count. An explicit YAML or code label always wins, without
reading the package. Missing or unrecognized metadata remains unknown;
unreadable or malformed metadata emits a warning without blocking registration.
Detection stops at the nearest package even when that package yields no label.
The description is stored once for local and linked observations, with no
request-path filesystem access, module imports or changes to execution support.

JavaScript follows the same configuration boundary as persistent PHP
applications. `config/js.yaml` owns the runtime and its named applications:
`engine` contains process-wide engine policy, while `applications` maps stable
names to an entry `module`, fixed per-worker `instances`, and explicit
application-local `env`. `environment` remains a compatibility alias for
`env`. `env_file` is one path or an ordered list; Dotenw applies each selected
deployment variant and then inline overrides. Values are frozen into each
isolate and are available through
`claviron:runtime`; the process environment is never exposed implicitly.
The application service selects env notifications inside its existing runnable.
Changed effective values renew only that named application through its joined
stop/start path. Stopped applications retain their stopped state and use the
latest valid values on their next start; invalid sources retain live execution.
`engine.command_queue_capacity` bounds only the non-HTTP commands waiting in
each isolate's mailbox and defaults to 64. HTTP dispatch enters the same FIFO
without reserving a second capacity permit: the application's `max_waiting`
already bounds these requests until guest completion. Taking a control command
from the mailbox releases its capacity permit; its callback may remain active.
`engine.websocket` carries the transport limits the runtime applies around a
socket rather than inside the isolate:
`max_message_size` is the ceiling the frame collector reassembles to and the
size above which an inbound message is refused with 1009, and
`action_queue_capacity` bounds the actions one application may buffer toward
one socket before it waits. Both are engine-wide for the same reason `fetch`
is: they describe what the runtime does on the application's behalf, not how
one application is dispatched. `config/js.yaml` spells out every accepted key
at its default value, so the whole surface is readable without the schema. Every application separately declares its fixed
request `dispatch` algorithm and execution limits. `round_robin` selects the
next live instance without allocating a temporary list on each dispatch.
`max_waiting` bounds queued and executing requests, including live response
producers after their headers have been sent; its owned permit is released with
actual guest completion. `requests.queue_timeout` bounds the wait for the
isolate owner's acknowledgement, including time queued behind earlier commands.
A zero `requests.queue_timeout` enqueues immediately without waiting for the
owner acknowledgement; HTTP has no separate mailbox-capacity rejection.
When both queue and execution timeouts are unset, HTTP allocates no acceptance
oneshot and waits only for its response. A positive queue timeout, or an
execution timeout with an unset queue timeout, retains owner acknowledgement.
This keeps execution's deadline after acceptance. A zero queue timeout also
needs no acknowledgement; its execution budget begins after enqueue as before.
The shared application admission bound intentionally includes cancelled queued
commands until their owner consumes or drops them. A slow isolate's backlog
can therefore exhaust admission for healthy siblings; round-robin does not
reserve a separate budget per isolate. Cancelled commands are skipped before
guest entry, releasing their owned permit and active counter exactly once.
`execution_timeout` bounds acceptance to the response head and cancels
only that request cooperatively; `null` leaves it unbounded. It never invokes
V8's isolate-wide `terminate_execution` for an individual request. A synchronous
non-yielding handler cannot be safely preempted in isolation from its peers.
No new hard interruption policy or duration is inferred.

Each application also declares `retry`, the same `max_attempts` / `backoff` /
`backoff_max` a PHP application declares, resolving to the shared
`claviron-app::ApplicationRetryPolicy`. Instance replacement is this crate's
own job: a slot whose isolate owner fails to boot, or dies afterwards, backs
off by `backoff * 2^(failures - 1)` capped at `backoff_max`, and is left down
once `max_attempts` consecutive failures pass with no completed request.
Completing a request clears the counter, so only a loop without progress keeps
backing off. A replacement is a new generation of an instance; an accepted
request is never replayed into it. Dispatch skips a slot in backoff rather
than writing into a closed mailbox, and an application with no live slot
rejects dispatch as not running while it recovers.

This supersedes the earlier rule that a boot failure fails the pool and leaves
recovery to the supervisor. It did not survive contact with the requirement
that the two application runtimes behave alike: the supervisor restarts a
process, which is the wrong granularity for one isolate out of several, and it
left `backing_off` and `exhausted` permanently unreportable on this side.

Typed HTTP declarations do not define or mutate a JavaScript application. Their
entries only bind an optional HTTP `path` to an application name
from `js.yaml`, plus exact WebSocket route paths because those are HTTP routing
data. This is the same separation used by PHP: runtime/application ownership
stays outside the vhost, and one named application may be mapped without
duplicating its engine definition across sites.

WebSocket route paths are relative to the application mount: `/api` plus
`/socket` becomes `/api/socket`. The host must separately opt into H1 protocol
upgrades with `transport.h1.upgrade`. The former flat upgrade field is no
longer accepted. This keeps the host transport gate distinct from the
application capability; the host's protocol selector must also allow H1.

At worker registration, `js` publishes one `JsApplicationService` and the
`JsConfigProvider` that owns `js.yaml` through Congif: the file is prepared —
variables from the application's frozen configuration environment — before
any provider boots, and `configure` hands the service its engine settings and
declares its applications, whose pools the service generation then boots. An
absent file declares no application. A change prepared while a generation
runs applies its engine settings to the next one; a new or changed application
is refused until the service restarts, and the refusal says so. During
router construction, the consuming application passes typed declarations to
`claviron_deno::routing::build_router`. The JavaScript crate resolves each route's
application name against that service and adds prefix-aware request layers.
The existing single-mount builder remains re-exported at the crate root.
A missing name is
unavailable; routing never manufactures a second application definition.
Longer application prefixes win. Explicit static mounts remain outside
application layers, so a frontend can be served directly while `/api` is
handled by JavaScript.

With `edge`, the same `register(state)` installs `routing::edge::JsRouteExtension`
in the existing `HostAppRegistry` when that registry is present. It compiles
the `js:` directive at host snapshot preparation, capturing the declared
application service and request/response policy in the router. No YAML parse,
service lookup or additional prefix selection occurs during HTTP dispatch.
An unregistered service, undeclared application, typo or duplicate application
owner fails snapshot preparation; no alternative application is manufactured.
The route map owns the prefix, so `path` inside the directive is rejected:

```yaml
example.test:
  transport:
    h1:
      upgrade: true
    h2:
      connect_protocol: true
  routes:
    /api:
      js:
        application: example
        websockets:
          - path: /socket
```

Flat `js: { application: example }` binds the implicit `/` owner. With the
`websocket` feature, declared exact endpoints are mounted relative to that
owner (`/api/socket` above). The compiler wraps its router with the existing
Claviron H1 upgrade middleware and a candidate-local registry. The same exact
bindings accept H2 WebSockets when `transport.h2.connect_protocol` is enabled.
The H2 hand-off is local to this router snapshot, preserving its ordinary
policy layers; no live global CONNECT binding is mutated during preparation.
Failed or repeated preparation never mutates live bindings. Extended CONNECT
must carry protocol `websocket` and WebSocket version 13; it returns 200 with
no H1 upgrade/accept headers. Its method and URI are never rewritten for Fetch.
Both entrances reserve an instance through `prepare_websocket` and run the same
WebSocket event/frame loop through the listener's `SessionSpawner`. Native drain
sends close 1001; no private deadline or detached task is introduced.
WebSocket declarations without the feature or without `transport.h1.upgrade`
fail preparation rather than being ignored. This directive does not register
H3 WebTransport endpoints.

The service is registered only on workers and standalone processes; the master
is not a JavaScript placement. Stop drains each mailbox, invokes the optional
application shutdown hook and joins every V8 owner thread; a later start boots
a fresh generation from the retained definitions. Each owner thread drives
both its command mailbox and the V8 event
loop. Detached promises and timers therefore continue while no HTTP, WebSocket,
or fleet-event command is arriving; incoming commands can still interrupt the
event-loop wait and retain single-threaded isolate ownership. Non-HTTP callback promises are
tracked by the same owner so an asynchronous event callback does not monopolize
HTTP acceptance. Shutdown closes admission, signals accepted calls through their AbortSignals
and drains them before invoking the application cleanup hook. Pending guest
work remains owned under the declared graceful policy; background timers are
left for the cleanup hook before the final event-loop drain.

`JsApplicationService` is also the authoritative owner of each named
application's desired enabled state, lifecycle, generation, live pool and last
boot error. Its `JsApplicationController` registers with the process-local
`claviron-app::ApplicationManager`; the common manager never duplicates that
state. Named start, stop and restart operations are serialized with aggregate
service startup and drain. They create or drain only the selected application
pool, and emit management lifecycle events only after the corresponding engine
transition completes. A stopped aggregate service cannot be bypassed through
the application controller. `instances`, `ready`, `busy`, `backing_off` and
`exhausted` carry the same meaning they do for PHP; `retiring` stays `None`
because JavaScript has no slot rotation to report, and a counter this engine
does not measure stays `None` rather than being reported as zero.

## Fleet cells

`import { Orbital } from "claviron:orbit/cell"` exposes typed shared values.
`Orbital.int()`, `.float()`, `.bool()` and `.string()` start at `0n`, `0`,
`false` and `""`; each accepts an optional initial value. Application code
does not create tables or register a cell provider. Claviron's fleet setup
prepares the common cell tables before an isolate runs, and the Orbit extension
uses that same provider. A factory allocates only a slot in those tables.
Allocation failure throws rather than producing an unshared fallback value.
`get()`, `set()` and `add()` use the shared slot; `open(id)` reattaches to it.
The existing explicit `release()` contract is unchanged, so passing an id
between language runtimes does not introduce a new ownership protocol.

Scalar `version()` and `changed()` tokens remain JavaScript Numbers bounded
by `u32`; string-cell tokens are BigInts carrying the complete `u64` revision.
The private op boundary transports revisions as decimal strings, preserving
all bits without JavaScript Number coercion. Invalid or out-of-range wait
tokens are rejected rather than wrapped.

## Runtime boundary

This is an embedded `deno_core` engine, not a full `deno_runtime::MainWorker`
or another standalone JavaScript runner. Standalone JavaScript applications
use the existing Orbit JS package; this crate is primarily for applications
embedding JavaScript at the edge. Nothing prevents a consumer from using the
same application adapter in a runner, but no second runner is introduced here.

For embedded scripts, `task::executor()` supplies the existing
`claviron::tasks::Executor` contract. A consumer can install it on TaskService
with `set_executor(TaskExecutorKind::Js, ...)`; `task::register(state)` is the
convenience form when TaskService is already registered. The scheduler still
owns task configuration, retries and cancellation. No new registration trait
or scheduler is required.

## Consumption boundary

This is an independent Cargo package, outside Claviron's workspace. Applications
consume `claviron-deno` directly and select its features. Its Claviron dependency
uses the public facade only; no internal `crates/*` paths are dependencies.
The dependency direction is application → claviron-deno → claviron. Claviron
and Edge never select Deno on the consumer's behalf.

Consumers explicitly register the runtime and compose routing and protocol
upgrade bindings using the existing public adapters. `server` provides the
application service and Axum mount builders without choosing edge or runner
topology. The `js.yaml`, `js:` task key and `claviron:*` JavaScript module names
remain unchanged; the Rust package rename is not a wire/config rename.

Direct dependencies use compatible semver ranges, not additional exact pins.
Deno's own crates may still require exact versions internally. In particular,
the selected deno_tls 0.247 line pins rustls 0.23.40. Widening this package's
deno_tls requirement does not override that upstream constraint. `fetch` owns
that outbound stack; Claviron retains ownership of its inbound TLS stack.
