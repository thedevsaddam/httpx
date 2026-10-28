// Package httpx is a small fluent HTTP client on top of net/http, with
// retries, request tracing and typed response helpers.
package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const Version = "v1.0.3"

const (
	defaultTimeout     = 30 * time.Second
	defaultMaxResponse = 10 << 20 // 10 MiB, applies to ToJSON/ToBytes/ToString
	errorBodyExcerpt   = 4 << 10  // how much of an error body HTTPError keeps
	drainLimit         = 4 << 10  // drained on Close so the connection can be reused
	headerContentType  = "Content-Type"
	contentTypeJSON    = "application/json"
	contentTypeForm    = "application/x-www-form-urlencoded"
	contentTypeOctet   = "application/octet-stream"
	methodQuery        = "QUERY"
)

// ErrEmptyBody is returned by ToJSON for a successful response with no body (a 204, say).
var ErrEmptyBody = errors.New("httpx: empty response body")

// Logger is an optional structured logger.
type Logger interface {
	Debug(ctx context.Context, msg string, keysAndValues ...any)
	Info(ctx context.Context, msg string, keysAndValues ...any)
	Error(ctx context.Context, msg string, keysAndValues ...any)
}

// Tracer injects trace context into outgoing requests.
type Tracer interface {
	Inject(ctx context.Context, req *http.Request)
}

// Auth adds credentials to outgoing requests.
type Auth interface {
	Apply(req *http.Request) error
}

// Middleware wraps a RoundTripper. The first middleware given is the outermost.
type Middleware func(http.RoundTripper) http.RoundTripper

// Client is safe for concurrent use once built with New.
type Client struct {
	httpClient       *http.Client
	baseURL          *url.URL
	headers          http.Header
	queryParams      url.Values
	auth             Auth
	logger           Logger
	tracer           Tracer
	timeout          time.Duration
	maxResponseBytes int64
	retryPolicy      RetryPolicy
	requestHook      RequestHook
	responseHook     ResponseHook
	initErr          error // bad config, surfaced by Do

	// Recorded by options and applied together at the end of New,
	// which is why option order doesn't matter.
	userClient    *http.Client
	rawBaseURL    string
	transport     http.RoundTripper
	pool          *PoolConfig
	tlsConfig     *tls.Config
	proxy         func(*http.Request) (*url.URL, error)
	proxySet      bool
	middlewares   []Middleware
	checkRedirect func(*http.Request, []*http.Request) error
	jar           http.CookieJar
	timeoutSet    bool
}

// Response wraps http.Response.
type Response struct {
	*http.Response
	trace        *TraceInfo
	maxBodyBytes int64
	method       string
	safeURL      string
	errTarget    any
}

// TraceInfo holds timings collected through net/http/httptrace. They're
// best-effort: some stay zero on a reused connection, behind a proxy, or when
// the transport doesn't fire the hook. TotalTime stops when the response
// headers arrive. URL never carries the query string since it may hold secrets.
type TraceInfo struct {
	Method         string
	URL            string
	Path           string
	StatusCode     int
	DNSLookup      time.Duration
	ConnTime       time.Duration
	TCPConnTime    time.Duration
	TLSHandshake   time.Duration
	ServerTime     time.Duration
	ResponseTime   time.Duration
	TotalTime      time.Duration
	IsConnReused   bool
	IsConnWasIdle  bool
	ConnIdleTime   time.Duration
	RequestAttempt int
	RemoteAddr     string

	mu             sync.Mutex
	dnsStart       time.Time
	connectStart   time.Time
	tlsStart       time.Time
	requestWritten time.Time
	firstByte      time.Time
}

// Snapshot returns a copy that is safe to read while the request is still running.
func (t *TraceInfo) Snapshot() TraceInfo {
	if t == nil {
		return TraceInfo{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return TraceInfo{
		Method: t.Method, URL: t.URL, Path: t.Path, StatusCode: t.StatusCode,
		DNSLookup: t.DNSLookup, ConnTime: t.ConnTime, TCPConnTime: t.TCPConnTime,
		TLSHandshake: t.TLSHandshake, ServerTime: t.ServerTime, ResponseTime: t.ResponseTime,
		TotalTime: t.TotalTime, IsConnReused: t.IsConnReused,
		IsConnWasIdle: t.IsConnWasIdle, ConnIdleTime: t.ConnIdleTime,
		RequestAttempt: t.RequestAttempt, RemoteAddr: t.RemoteAddr,
	}
}

func (t *TraceInfo) String() string {
	if t == nil {
		return "Request Trace Info: <unavailable>"
	}
	s := t.Snapshot()
	return fmt.Sprintf(
		"Request Trace Info:\n"+
			"  Method         : %s\n"+
			"  URL            : %s\n"+
			"  Path           : %s\n"+
			"  StatusCode     : %d\n"+
			"  DNSLookup      : %s\n"+
			"  ConnTime       : %s\n"+
			"  TCPConnTime    : %s\n"+
			"  TLSHandshake   : %s\n"+
			"  ServerTime     : %s\n"+
			"  ResponseTime   : %s\n"+
			"  TotalTime      : %s\n"+
			"  IsConnReused   : %t\n"+
			"  IsConnWasIdle  : %t\n"+
			"  ConnIdleTime   : %s\n"+
			"  RequestAttempt : %d\n"+
			"  RemoteAddr     : %s",
		s.Method, s.URL, s.Path, s.StatusCode, s.DNSLookup, s.ConnTime,
		s.TCPConnTime, s.TLSHandshake, s.ServerTime, s.ResponseTime,
		s.TotalTime, s.IsConnReused, s.IsConnWasIdle, s.ConnIdleTime,
		s.RequestAttempt, s.RemoteAddr,
	)
}

// Trace returns a snapshot of the request timings.
func (r *Response) Trace() TraceInfo {
	if r == nil {
		return TraceInfo{}
	}
	return r.trace.Snapshot()
}

// Debug returns the trace as a printable report.
func (r *Response) Debug() string {
	if r == nil {
		return "Request Trace Info: <unavailable>"
	}
	return r.trace.String()
}

// HTTPError is what you get for a non-2xx response. Body is cut off at about 4 KiB
// and URL has no query string.
type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
	Header     http.Header
	Body       string
	// Decoded is true when the body was decoded into the value given to ErrorInto.
	// Only JSON responses are decoded (see ErrorInto).
	Decoded bool
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("httpx: %s %s: unexpected status code %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// RequestHook runs right before every attempt. It must be safe for concurrent use.
type RequestHook func(context.Context, *http.Request)

// ResponseHook runs after the headers of every attempt arrive. It must be safe for concurrent use.
type ResponseHook func(context.Context, *http.Response)
