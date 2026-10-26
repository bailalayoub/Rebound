package sseclient

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

// Frame is one block of fields terminated by a blank line, parsed per the WHATWG
// "event stream interpretation" rules but kept raw enough for tests to see comments and retry.
type Frame struct {
	ID       string // last id field; meaningful when HasID
	HasID    bool
	Event    string // event field; "" when absent
	Data     string // data fields joined with "\n"
	HasData  bool
	Retry    int64 // retry field in milliseconds; meaningful when HasRetry
	HasRetry bool
	Comments []string
}

// IsEvent reports whether the frame would be dispatched as an event by a browser (it has data).
func (f Frame) IsEvent() bool { return f.HasData }

// Reader parses an event stream.
type Reader struct {
	r       *bufio.Reader
	started bool
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// Next returns the next complete frame. At the end of the stream it returns io.EOF; a frame cut
// off by the end of the stream is discarded, as browsers do. Other read errors are returned as is.
func (rd *Reader) Next() (Frame, error) {
	var f Frame
	var data []string
	pending := false
	for {
		line, err := rd.readLine()
		if err != nil {
			return Frame{}, err
		}
		if line == "" {
			if !pending {
				continue
			}
			if f.HasData {
				f.Data = strings.Join(data, "\n")
			}
			return f, nil
		}
		pending = true
		name, value, found := strings.Cut(line, ":")
		if found && strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch name {
		case "":
			f.Comments = append(f.Comments, value)
		case "id":
			if !strings.ContainsRune(value, 0) {
				f.ID, f.HasID = value, true
			}
		case "event":
			f.Event = value
		case "data":
			data = append(data, value)
			f.HasData = true
		case "retry":
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil && isDigits(value) {
				f.Retry, f.HasRetry = ms, true
			}
		}
	}
}

// readLine returns one line without its terminator (CRLF, LF or CR), stripping a leading BOM.
func (rd *Reader) readLine() (string, error) {
	var b bytes.Buffer
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			return "", err
		}
		switch c {
		case '\n':
			return rd.finish(b.String()), nil
		case '\r':
			if next, err := rd.r.Peek(1); err == nil && next[0] == '\n' {
				if _, err := rd.r.ReadByte(); err != nil {
					return "", err
				}
			}
			return rd.finish(b.String()), nil
		default:
			b.WriteByte(c)
		}
	}
}

func (rd *Reader) finish(line string) string {
	if !rd.started {
		rd.started = true
		line = strings.TrimPrefix(line, "\uFEFF")
	}
	return line
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}
