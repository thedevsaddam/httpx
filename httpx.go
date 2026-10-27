// Package httpx provides a fluent HTTP client with retries, request tracing and response helpers.
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

const Version = "v1.0.1"

const (
	defaultTimeout     = 30 * time.Second
	defaultMaxResponse = 10 << 20 // 10 MiB for ToJSON/ToBytes/ToString
	errorBodyExcerpt   = 4 << 10  // bytes of an error response kept in HTTPError.Body
	drainLimit         = 4 << 10  // bytes drained on Close to allow connection reuse
	headerContentType  = "Content-Type"
	contentTypeJSON    = "application/json"
	contentTypeForm    = "application/x-www-form-urlencoded"
	contentTypeOctet   = "application/octet-stream"
	methodQuery        = "QUERY"
)

// ErrEmptyBody is returned by ToJSON when a successful response has no body (for example 204).
var ErrEmptyBody = errors.New("httpx: empty response body")

// Logger is the optional structured logging interface.
type Logger interface {
	Debug(ctx context.Context, msg string, keysAndValues ...any)
	Info(ctx context.Context, msg string, keysAndValues ...any)
	Error(ctx context.Context, msg string, keysAndValues ...any)
}

// Tracer injects distributed tracing information into an outgoing request.
type Tracer interface {
	Inject(ctx context.Context, req *http.Request)
}

// Auth injects authentication into an outgoing request.
type Auth interface {
	Apply(req *http.Request) error
}

// Middleware wraps a RoundTripper, for example to add OpenTelemetry, metrics or rate limiting.
// Middlewares run in the order given: the first one is the outermost.
type Middleware func(http.RoundTripper) http.RoundTripper

// Client wraps http.Client and provides defaults and fluent request builders.
// A Client is safe for concurrent use once constructed.
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
	initErr          error // configuration error reported by Send

	// Collected by options and applied once at the end of New, so option order does not matter.
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

// Response wraps http.Response and exposes trace information and helpers.
type Response struct {
	*http.Response
	trace        *TraceInfo
	maxBodyBytes int64
	method       string
	safeURL      string
}

// TraceInfo contains timing and connection information collected with net/http/httptrace.
// Durations are best-effort and some fields remain zero when a connection is reused,
// a proxy is involved, or the transport does not emit the corresponding hook.
// TotalTime measures the time until response headers are available.
// URL never includes the query string, which may carry secrets.
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
	gotConnAt      time.Time
	startedAt      time.Time
}

// Snapshot returns a copy of the trace fields, safe to inspect while the request is running.
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

// String formats the trace in a human-readable diagnostic report.
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

// Trace returns a snapshot of the request timing information.
func (r *Response) Trace() TraceInfo {
	if r == nil || r.trace == nil {
		return TraceInfo{}
	}
	return r.trace.Snapshot()
}

// Debug returns a formatted trace report for this response.
func (r *Response) Debug() string {
	if r == nil {
		return "Request Trace Info: <unavailable>"
	}
	return r.trace.String()
}

// HTTPError represents a non-2xx HTTP response. Body is a bounded excerpt (about 4 KiB).
// URL never includes the query string.
type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
	Header     http.Header
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("httpx: %s %s: unexpected status code %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// RequestHook runs immediately before each network attempt. Hooks must be safe for concurrent calls.
type RequestHook func(context.Context, *http.Request)

// ResponseHook runs after response headers arrive on each attempt. Hooks must be safe for concurrent calls.
type ResponseHook func(context.Context, *http.Response)

// RetryPolicy controls retries. MaxAttempts includes the first attempt. A value below 2
// means no retries. Jitter enables full jitter: each backoff is randomized between zero
// and the exponential backoff cap. When RespectRetryAfter is set and the server asks for
// a delay longer than MaxBackoff (when positive), the request is not retried and the
// response is returned to the caller instead of hammering the server too early.
type RetryPolicy struct {
	MaxAttempts          int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	RetryableStatusCodes []int
	RetryNetworkErrors   bool
	Jitter               bool
	RespectRetryAfter    bool
}

// StandardRetryPolicy returns a conservative policy for common transient failures.
// It makes up to three total attempts, retries network errors and HTTP 429/502/503/504,
// uses capped exponential backoff with full jitter, and honors Retry-After when present.
func StandardRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          3,
		InitialBackoff:       200 * time.Millisecond,
		MaxBackoff:           2 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}
