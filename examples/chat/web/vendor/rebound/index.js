/**
 * `@bailalayoub/rebound-client`: a fetch-based, resumable Server-Sent Events client for Rebound streams.
 * Zero dependencies; works in browsers and Node 18+.
 *
 * @example
 * const sub = subscribe("/streams/abc", {
 *   onEvent: (e) => console.log(e.id, e.kind, e.data),
 *   onDone: () => console.log("complete"),
 * });
 */
export { subscribe } from "./client.js";
export { SSEParser } from "./parser.js";
//# sourceMappingURL=index.js.map