// Package httpx serves Rebound streams over net/http: GET /streams/{id} resumes from the
// Last-Event-ID cursor and follows the stream as SSE, POST /streams/{id}/cancel cancels it.
// It maps library errors to HTTP statuses (204, 400, 404, 410, 401/403) as in docs/02-spec.md §4.
package httpx
