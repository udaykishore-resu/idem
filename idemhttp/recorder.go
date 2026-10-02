package idemhttp

import (
	"bytes"
	"net/http"
	"slices"
)

// recorder is the ResponseWriter handed to the wrapped handler. It holds the
// response back until the handler returns, so that the outcome can be stored
// before the client sees it.
//
// If the body grows past limit, the recorder gives up on storing it: it sends
// what it has and writes the rest straight through.
type recorder struct {
	w      http.ResponseWriter
	limit  int64
	before http.Header // headers already set when the handler was called
	header http.Header // headers as the handler sees and edits them
	sent   http.Header // headers frozen by WriteHeader, as net/http would send them
	status int
	buf    bytes.Buffer

	overflowed bool
}

func newRecorder(w http.ResponseWriter, limit int64) *recorder {
	return &recorder{
		w:      w,
		limit:  limit,
		before: w.Header().Clone(),
		header: w.Header().Clone(),
	}
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(status int) {
	if r.status != 0 || status < 200 {
		// A second call is a handler bug that net/http also ignores.
		// Informational responses cannot be held back and are dropped.
		return
	}
	r.status = status
	r.sent = r.header.Clone()
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	if r.overflowed {
		return r.w.Write(p)
	}
	if int64(r.buf.Len())+int64(len(p)) > r.limit {
		// From here on the response is the client's alone. Mark it first:
		// if the writes below fail, what is left in the buffer must not be
		// taken for a whole response.
		r.overflowed = true
		held := r.buf.Bytes()
		r.sendHead()
		if _, err := r.w.Write(held); err != nil {
			return 0, err
		}
		return r.w.Write(p)
	}
	return r.buf.Write(p)
}

// wrote reports whether the handler has written a status or a body.
func (r *recorder) wrote() bool { return r.status != 0 }

// statusCode returns the status the handler chose, or 200 if it returned
// without writing anything, as net/http would send.
func (r *recorder) statusCode() int {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	return r.status
}

// body returns the buffered body, or nil if it overflowed.
func (r *recorder) body() []byte {
	if r.overflowed {
		return nil
	}
	return r.buf.Bytes()
}

// sendHead writes the frozen headers and the status to the real writer.
func (r *recorder) sendHead() {
	dst := r.w.Header()
	clear(dst)
	for name, values := range r.sent {
		dst[name] = values
	}
	r.w.WriteHeader(r.status)
}

// flush sends the buffered response to the client, unless overflow has sent
// it already.
func (r *recorder) flush() {
	if r.overflowed {
		return
	}
	r.statusCode()
	r.sendHead()
	if r.buf.Len() > 0 {
		_, _ = r.w.Write(r.buf.Bytes())
	}
}

// notStored lists headers that describe one particular connection or moment
// and must not be copied onto a replayed response.
var notStored = map[string]bool{
	"Connection":        true,
	"Date":              true,
	"Keep-Alive":        true,
	"Te":                true,
	"Trailer":           true,
	"Transfer-Encoding": true,
	"Upgrade":           true,
}

// handlerHeaders returns the headers the handler added or changed. Headers
// that were set before the handler ran, by middleware further out, are left
// out: on a replay that middleware sets them afresh.
func (r *recorder) handlerHeaders() http.Header {
	r.statusCode()
	added := make(http.Header)
	for name, values := range r.sent {
		if notStored[name] || slices.Equal(values, r.before[name]) {
			continue
		}
		added[name] = values
	}
	return added
}
