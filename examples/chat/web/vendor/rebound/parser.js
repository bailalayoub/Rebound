/**
 * Incremental SSE parser: feed it text chunks as they arrive from the network, in any sizes,
 * and it calls `onFrame` for each complete frame. Lines end with CRLF, LF or CR, including a CRLF
 * split across two chunks. A frame cut off by the end of the stream is discarded, as browsers do.
 */
export class SSEParser {
    constructor(onFrame) {
        this.onFrame = onFrame;
        this.buffer = "";
        this.frame = SSEParser.emptyFrame();
        this.data = [];
        this.pending = false;
        this.started = false;
        this.lastWasCR = false;
    }
    /** Parses a chunk of the stream. */
    feed(chunk) {
        let text = chunk;
        if (!this.started && text.length > 0) {
            this.started = true;
            if (text.charCodeAt(0) === 0xfeff) {
                text = text.slice(1);
            }
        }
        if (this.lastWasCR && text.startsWith("\n")) {
            text = text.slice(1); // second half of a CRLF split across chunks
        }
        this.lastWasCR = false;
        this.buffer += text;
        let start = 0;
        for (let i = 0; i < this.buffer.length; i++) {
            const c = this.buffer.charCodeAt(i);
            if (c !== 0x0a && c !== 0x0d) {
                continue;
            }
            this.line(this.buffer.slice(start, i));
            if (c === 0x0d) {
                if (i + 1 < this.buffer.length) {
                    if (this.buffer.charCodeAt(i + 1) === 0x0a) {
                        i++;
                    }
                }
                else {
                    this.lastWasCR = true;
                }
            }
            start = i + 1;
        }
        this.buffer = this.buffer.slice(start);
    }
    /** Discards any incomplete frame, for example after the connection dropped. */
    reset() {
        this.buffer = "";
        this.frame = SSEParser.emptyFrame();
        this.data = [];
        this.pending = false;
        this.lastWasCR = false;
    }
    line(line) {
        if (line === "") {
            if (this.pending) {
                const frame = this.frame;
                frame.data = this.data.join("\n");
                this.frame = SSEParser.emptyFrame();
                this.data = [];
                this.pending = false;
                this.onFrame(frame);
            }
            return;
        }
        this.pending = true;
        const colon = line.indexOf(":");
        let name = line;
        let value = "";
        if (colon >= 0) {
            name = line.slice(0, colon);
            value = line.slice(colon + 1);
            if (value.startsWith(" ")) {
                value = value.slice(1);
            }
        }
        switch (name) {
            case "":
                this.frame.comments.push(value);
                break;
            case "id":
                if (!value.includes("\u0000")) {
                    this.frame.id = value;
                }
                break;
            case "event":
                this.frame.event = value;
                break;
            case "data":
                this.data.push(value);
                this.frame.hasData = true;
                break;
            case "retry":
                if (/^[0-9]+$/.test(value)) {
                    this.frame.retry = Number(value);
                }
                break;
            default:
                // Unknown fields are ignored (WHATWG).
                break;
        }
    }
    static emptyFrame() {
        return { data: "", hasData: false, comments: [] };
    }
}
//# sourceMappingURL=parser.js.map