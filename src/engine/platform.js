import { core } from "ext:core/mod.js";

const url = core.loadExtScript("ext:deno_web/00_url.js");
const event = core.loadExtScript("ext:deno_web/02_event.js");
const abortSignal = core.loadExtScript("ext:deno_web/03_abort_signal.js");
const { DOMException } = core.loadExtScript("ext:deno_web/01_dom_exception.js");
const file = core.loadExtScript("ext:deno_web/09_file.js");
const performance = core.loadExtScript("ext:deno_web/15_performance.js");
const console = core.loadExtScript("ext:deno_web/01_console.js");
const crypto = core.loadExtScript("ext:deno_crypto/00_crypto.js");

const loadBase64 = () => core.loadExtScript("ext:deno_web/05_base64.js");
const loadEncoding = () => core.loadExtScript("ext:deno_web/08_text_encoding.js");
const loadStreams = () => core.loadExtScript("ext:deno_web/06_streams.js");
const loadTimers = () => core.loadExtScript("ext:deno_web/02_timers.js");
const loadUrlPattern = () => core.loadExtScript("ext:deno_web/01_urlpattern.js");
const loadCompression = () => core.loadExtScript("ext:deno_web/14_compression.js");
const loadFileReader = () => core.loadExtScript("ext:deno_web/10_filereader.js");
const loadBroadcastChannel = () =>
    core.loadExtScript("ext:deno_web/01_broadcast_channel.js");
const loadMessagePort = () => core.loadExtScript("ext:deno_web/13_message_port.js");

Object.defineProperties(globalThis, {
    AbortController: core.propNonEnumerable(abortSignal.AbortController),
    AbortSignal: core.propNonEnumerable(abortSignal.AbortSignal),
    Blob: core.propNonEnumerable(file.Blob),
    BroadcastChannel: core.propNonEnumerableLazyLoaded(
        (module) => module.BroadcastChannel,
        loadBroadcastChannel,
    ),
    ByteLengthQueuingStrategy: core.propNonEnumerableLazyLoaded(
        (module) => module.ByteLengthQueuingStrategy,
        loadStreams,
    ),
    CloseEvent: core.propNonEnumerable(event.CloseEvent),
    CompressionStream: core.propNonEnumerableLazyLoaded(
        (module) => module.CompressionStream,
        loadCompression,
    ),
    CountQueuingStrategy: core.propNonEnumerableLazyLoaded(
        (module) => module.CountQueuingStrategy,
        loadStreams,
    ),
    Crypto: core.propNonEnumerable(crypto.Crypto),
    CryptoKey: core.propNonEnumerable(crypto.CryptoKey),
    SubtleCrypto: core.propNonEnumerable(crypto.SubtleCrypto),
    CustomEvent: core.propNonEnumerable(event.CustomEvent),
    DecompressionStream: core.propNonEnumerableLazyLoaded(
        (module) => module.DecompressionStream,
        loadCompression,
    ),
    DOMException: core.propNonEnumerable(DOMException),
    ErrorEvent: core.propNonEnumerable(event.ErrorEvent),
    Event: core.propNonEnumerable(event.Event),
    EventTarget: core.propNonEnumerable(event.EventTarget),
    File: core.propNonEnumerable(file.File),
    FileReader: core.propNonEnumerableLazyLoaded(
        (module) => module.FileReader,
        loadFileReader,
    ),
    MessageChannel: core.propNonEnumerableLazyLoaded(
        (module) => module.MessageChannel,
        loadMessagePort,
    ),
    MessageEvent: core.propNonEnumerable(event.MessageEvent),
    MessagePort: core.propNonEnumerableLazyLoaded(
        (module) => module.MessagePort,
        loadMessagePort,
    ),
    Performance: core.propNonEnumerable(performance.Performance),
    PerformanceEntry: core.propNonEnumerable(performance.PerformanceEntry),
    PerformanceMark: core.propNonEnumerable(performance.PerformanceMark),
    PerformanceMeasure: core.propNonEnumerable(performance.PerformanceMeasure),
    PerformanceObserver: core.propNonEnumerable(performance.PerformanceObserver),
    PerformanceObserverEntryList: core.propNonEnumerable(
        performance.PerformanceObserverEntryList,
    ),
    PromiseRejectionEvent: core.propNonEnumerable(event.PromiseRejectionEvent),
    ProgressEvent: core.propNonEnumerable(event.ProgressEvent),
    ReadableStream: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableStream,
        loadStreams,
    ),
    ReadableByteStreamController: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableByteStreamController,
        loadStreams,
    ),
    ReadableStreamBYOBReader: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableStreamBYOBReader,
        loadStreams,
    ),
    ReadableStreamBYOBRequest: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableStreamBYOBRequest,
        loadStreams,
    ),
    ReadableStreamDefaultController: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableStreamDefaultController,
        loadStreams,
    ),
    ReadableStreamDefaultReader: core.propNonEnumerableLazyLoaded(
        (module) => module.ReadableStreamDefaultReader,
        loadStreams,
    ),
    TextDecoder: core.propNonEnumerableLazyLoaded(
        (module) => module.TextDecoder,
        loadEncoding,
    ),
    TextDecoderStream: core.propNonEnumerableLazyLoaded(
        (module) => module.TextDecoderStream,
        loadEncoding,
    ),
    TextEncoder: core.propNonEnumerableLazyLoaded(
        (module) => module.TextEncoder,
        loadEncoding,
    ),
    TextEncoderStream: core.propNonEnumerableLazyLoaded(
        (module) => module.TextEncoderStream,
        loadEncoding,
    ),
    TransformStream: core.propNonEnumerableLazyLoaded(
        (module) => module.TransformStream,
        loadStreams,
    ),
    TransformStreamDefaultController: core.propNonEnumerableLazyLoaded(
        (module) => module.TransformStreamDefaultController,
        loadStreams,
    ),
    URL: core.propNonEnumerable(url.URL),
    URLPattern: core.propNonEnumerableLazyLoaded(
        (module) => module.URLPattern,
        loadUrlPattern,
    ),
    URLSearchParams: core.propNonEnumerable(url.URLSearchParams),
    WritableStream: core.propNonEnumerableLazyLoaded(
        (module) => module.WritableStream,
        loadStreams,
    ),
    WritableStreamDefaultController: core.propNonEnumerableLazyLoaded(
        (module) => module.WritableStreamDefaultController,
        loadStreams,
    ),
    WritableStreamDefaultWriter: core.propNonEnumerableLazyLoaded(
        (module) => module.WritableStreamDefaultWriter,
        loadStreams,
    ),
    atob: core.propWritableLazyLoaded((module) => module.atob, loadBase64),
    btoa: core.propWritableLazyLoaded((module) => module.btoa, loadBase64),
    clearInterval: core.propWritableLazyLoaded(
        (module) => module.clearInterval,
        loadTimers,
    ),
    clearTimeout: core.propWritableLazyLoaded(
        (module) => module.clearTimeout,
        loadTimers,
    ),
    console: core.propNonEnumerable(
        new console.Console((message, level) => {
            if (!core.ops.op_claviron_console_write(message, level > 1)) {
                throw new Error("Claviron task console write failed");
            }
        }),
    ),
    crypto: core.propGetterOnly(() => crypto.crypto),
    performance: core.propWritable(performance.performance),
    reportError: core.propWritable(event.reportError),
    setInterval: core.propWritableLazyLoaded(
        (module) => module.setInterval,
        loadTimers,
    ),
    setTimeout: core.propWritableLazyLoaded(
        (module) => module.setTimeout,
        loadTimers,
    ),
    structuredClone: core.propWritableLazyLoaded(
        (module) => module.structuredClone,
        loadMessagePort,
    ),
});
