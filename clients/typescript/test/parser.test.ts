import { describe, expect, it } from "vitest";

import { SSEParser, type SSEFrame } from "../src/parser.js";

function parse(...chunks: string[]): SSEFrame[] {
  const frames: SSEFrame[] = [];
  const parser = new SSEParser((f) => frames.push(f));
  for (const c of chunks) {
    parser.feed(c);
  }
  return frames;
}

describe("SSEParser", () => {
  it("parses the Rebound wire format", () => {
    const frames = parse("retry: 1000\n\n: ping\n\nid: 42\nevent: data\ndata: {\"text\":\"Bon\"}\n\n");
    expect(frames).toHaveLength(3);
    expect(frames[0]).toMatchObject({ retry: 1000, hasData: false });
    expect(frames[1]).toMatchObject({ comments: ["ping"], hasData: false });
    expect(frames[2]).toEqual({ id: "42", event: "data", data: '{"text":"Bon"}', hasData: true, comments: [] });
  });

  it("joins multi-line data with LF whatever the line endings", () => {
    for (const input of [
      "data: a\ndata:\ndata: b\n\n",
      "data: a\r\ndata:\r\ndata: b\r\n\r\n",
      "data: a\rdata:\rdata: b\r\r",
    ]) {
      const frames = parse(input);
      expect(frames).toHaveLength(1);
      expect(frames[0]!.data).toBe("a\n\nb");
    }
  });

  it("handles a CRLF split across chunks", () => {
    const frames = parse("id: 1\r", "\ndata: x\r", "\n\r", "\n");
    expect(frames).toEqual([{ id: "1", data: "x", hasData: true, comments: [] }]);
  });

  it("gives the same frames whatever the chunking", () => {
    const stream = "﻿id: 1\nevent: meta\ndata: {\"i\":1}\n\n: ping\n\nid: 2\ndata: line1\r\ndata: line2\r\n\r\nid: 3\revent: done\rdata: {}\r\r";
    const whole = parse(stream);
    const bytewise = parse(...stream.split(""));
    expect(bytewise).toEqual(whole);
    expect(whole.map((f) => f.id)).toEqual(["1", undefined, "2", "3"]);
    expect(whole[0]!.event).toBe("meta");
    expect(whole[2]!.data).toBe("line1\nline2");
  });

  it("applies the WHATWG field rules", () => {
    const frames = parse(
      "data:no space\n\n",
      "data:  two spaces\n\n",
      "data\n\n",
      "id: a\u0000b\ndata: x\n\n",
      "retry: 1x\nretry: -5\ndata: y\n\n",
      "unknown: field\ndata: z\n\n",
      "\n\n\n",
      "data: cut off at the end",
    );
    expect(frames.map((f) => f.data)).toEqual(["no space", " two spaces", "", "x", "y", "z"]);
    expect(frames[3]!.id).toBeUndefined();
    expect(frames[4]!.retry).toBeUndefined();
    expect(frames[2]!.hasData).toBe(true);
  });

  it("forgets an incomplete frame on reset", () => {
    const frames: SSEFrame[] = [];
    const parser = new SSEParser((f) => frames.push(f));
    parser.feed("id: 9\ndata: partial");
    parser.reset();
    parser.feed("id: 1\ndata: fresh\n\n");
    expect(frames).toEqual([{ id: "1", data: "fresh", hasData: true, comments: [] }]);
  });
});
