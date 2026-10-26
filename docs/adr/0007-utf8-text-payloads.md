# ADR 0007 — Event payloads are UTF-8 text with LF line endings

Status: accepted · Date: 2026-10-11

## Context

Spec §1/§5 called payloads "opaque bytes". SSE cannot carry a bare CR, so the encoder turned CR and
CRLF into LF, and browsers replace invalid UTF-8 with U+FFFD: SSE clients received different bytes
than `Subscription` callers and than what the producer wrote. Rejecting such payloads would break
real producers (model output with CRLF, tokens that split a multi-byte character).

## Decision

The `Writer` normalizes every payload before storing it: invalid UTF-8 sequences become U+FFFD and
CRLF / CR become LF. The size limit applies to the normalized payload. What is stored is exactly
what every reader receives, over SSE or through `Subscription`. Binary data must be encoded by the
producer (for example base64 inside JSON).

## Consequences

- G2 holds byte for byte across transports.
- `WriteJSON` output is unaffected (JSON escapes CR and is valid UTF-8).
- Producers that relied on raw CR or binary bytes see them normalized; documented in spec §1.
