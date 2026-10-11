const resolved = new URL("../users?q=a b", "https://example.com/api/v1/");
if (resolved.href !== "https://example.com/api/users?q=a%20b") {
    throw new Error(`unexpected URL resolution: ${resolved.href}`);
}

if (typeof AbortController !== "function") {
    throw new Error("AbortController is unavailable");
}

if (typeof console.log !== "function" || typeof setTimeout !== "function") {
    throw new Error("console or timer APIs are unavailable");
}

await new Promise((resolve) => setTimeout(resolve, 1));

if (new TextDecoder().decode(new TextEncoder().encode("Claviron")) !== "Claviron") {
    throw new Error("Web text encoding APIs are unavailable");
}

const random = new Uint8Array(16);
crypto.getRandomValues(random);
if (random.every((byte) => byte === 0)) {
    throw new Error("Web Crypto randomness is unavailable");
}

const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode("Claviron")),
);
if (
    digest.byteLength !== 32 ||
    typeof crypto.randomUUID() !== "string" ||
    !(crypto.subtle instanceof SubtleCrypto)
) {
    throw new Error("Web Crypto APIs are unavailable");
}

export default () => new Response("ready");
