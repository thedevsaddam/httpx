// Package httpx provides a fluent HTTP client with request tracing and response helpers.
package httpx

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Version = "1.0.0"

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

// Client wraps http.Client and provides defaults and fluent request builders.
type Client struct {
	httpClient       *http.Client
	baseURL          string
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
}

// Response wraps http.Response and exposes trace information and helpers.
type Response struct {
	*http.Response
	trace        *TraceInfo
	maxBodyBytes int64
}

// TraceInfo contains timing and connection information collected with net/http/httptrace.
// Durations are best-effort and some fields remain zero when a connection is reused,
// a proxy is involved, or the transport does not emit the corresponding hook.
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

// Debug prints a formatted trace report for this response.
func (r *Response) Debug() string {
	if r == nil {
		return "Request Trace Info: <unavailable>"
	}
	return r.trace.String()
}

// HTTPError represents a non-2xx HTTP response. Body is a bounded diagnostic excerpt.
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
	URL        string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("httpx: unexpected status code %d: %s", e.StatusCode, e.Body)
}

// RequestHook runs immediately before each network attempt. Hooks must be safe for concurrent calls.
type RequestHook func(context.Context, *http.Request)

// ResponseHook runs after response headers arrive. Hooks must be safe for concurrent calls.
type ResponseHook func(context.Context, *http.Response)

// RetryPolicy controls retries. MaxAttempts includes the first attempt. A value below 2
// means no retries. Jitter enables full jitter: each backoff is randomized between zero
// and the exponential backoff cap. RespectRetryAfter honors Retry-After on retryable
// responses, bounded by MaxBackoff when it is positive.
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

func retryBackoff(policy RetryPolicy, retryNumber int, resp *http.Response, now time.Time) time.Duration {
	capDelay := policy.InitialBackoff
	if capDelay <= 0 {
		return 0
	}
	for i := 1; i < retryNumber; i++ {
		if capDelay > time.Duration(1<<63-1)/2 {
			capDelay = time.Duration(1<<63 - 1)
			break
		}
		capDelay *= 2
	}
	if policy.MaxBackoff > 0 && capDelay > policy.MaxBackoff {
		capDelay = policy.MaxBackoff
	}
	if policy.RespectRetryAfter && resp != nil {
		if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
			var serverDelay time.Duration
			if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
				if seconds > int64((time.Duration(1<<63-1))/time.Second) {
					serverDelay = time.Duration(1<<63 - 1)
				} else {
					serverDelay = time.Duration(seconds) * time.Second
				}
			} else if when, err := http.ParseTime(value); err == nil && when.After(now) {
				serverDelay = when.Sub(now)
			}
			if serverDelay > 0 {
				if policy.MaxBackoff > 0 && serverDelay > policy.MaxBackoff {
					serverDelay = policy.MaxBackoff
				}
				return serverDelay
			}
		}
	}
	if policy.Jitter && capDelay > 0 {
		return time.Duration(rand.Int63n(int64(capDelay)))
	}
	return capDelay
}

// PoolConfig configures the standard HTTP transport connection pool.
type PoolConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
}

// Option configures a Client.
type Option func(*Client)

// New creates a client with connection pooling enabled by default.
func New(options ...Option) *Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second, KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
		MaxIdleConns:      100, MaxIdleConnsPerHost: 10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	// In New()
	defaultHeaders := make(http.Header)
	defaultHeaders.Set("User-Agent", "httpx/"+Version)
	c := &Client{
		httpClient:       &http.Client{Transport: transport, Timeout: 30 * time.Second},
		headers:          defaultHeaders,
		queryParams:      make(url.Values),
		timeout:          30 * time.Second,
		maxResponseBytes: 10 << 20, // 10 MiB default for helper reads; streaming remains available.
	}
	for _, opt := range options {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// WithPoolConfig configures connection pooling. Zero values retain the transport defaults.
func WithPoolConfig(cfg PoolConfig) Option {
	return func(c *Client) {
		t, ok := c.httpClient.Transport.(*http.Transport)
		if !ok || t == nil {
			return
		}
		t = t.Clone()
		if cfg.MaxIdleConns > 0 {
			t.MaxIdleConns = cfg.MaxIdleConns
		}
		if cfg.MaxIdleConnsPerHost > 0 {
			t.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
		}
		if cfg.MaxConnsPerHost > 0 {
			t.MaxConnsPerHost = cfg.MaxConnsPerHost
		}
		if cfg.IdleConnTimeout > 0 {
			t.IdleConnTimeout = cfg.IdleConnTimeout
		}
		if cfg.TLSHandshakeTimeout > 0 {
			t.TLSHandshakeTimeout = cfg.TLSHandshakeTimeout
		}
		if cfg.ResponseHeaderTimeout > 0 {
			t.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
		}
		c.httpClient.Transport = t
	}
}

// WithMaxResponseBytes sets the maximum body size consumed by ToJSON and error-body helpers.
// Send still exposes a streaming body; callers can stream arbitrarily large responses themselves.
func WithMaxResponseBytes(n int64) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxResponseBytes = n
		}
	}
}

// WithRetryPolicy configures opt-in retries. Retries are limited to idempotent methods and replayable bodies.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(c *Client) {
		p.RetryableStatusCodes = append([]int(nil), p.RetryableStatusCodes...)
		c.retryPolicy = p
	}
}
func WithRequestHook(h RequestHook) Option   { return func(c *Client) { c.requestHook = h } }
func WithResponseHook(h ResponseHook) Option { return func(c *Client) { c.responseHook = h } }

// WithClientName sets a Header `X-Client-Name`.
func WithClientName(value string) Option {
	return func(c *Client) { c.headers.Set("X-Client-Name", value) }
}

// WithTransport sets a custom transport.
func WithTransport(transport *http.Transport) Option {
	return func(c *Client) {
		if transport != nil {
			c.httpClient.Transport = transport
		}
	}
}

// WithHTTPClient uses a custom *http.Client. Its timeout and transport settings are preserved.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
			c.timeout = client.Timeout
		}
	}
}

// WithTimeout sets the default timeout for requests.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.timeout = timeout
		c.httpClient.Timeout = timeout
	}
}

// WithBaseURL sets a base URL prepended to relative request paths.
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.baseURL = baseURL }
}

// WithHeader sets a default header.
func WithHeader(key, value string) Option {
	return func(c *Client) { c.headers.Set(key, value) }
}

// WithQueryParam sets a default query parameter.
func WithQueryParam(key, value string) Option {
	return func(c *Client) { c.queryParams.Add(key, value) }
}

// WithAuth sets default authentication.
func WithAuth(auth Auth) Option {
	return func(c *Client) { c.auth = auth }
}

// WithLogger sets structured logging.
func WithLogger(logger Logger) Option {
	return func(c *Client) { c.logger = logger }
}

// WithTracer sets distributed trace propagation.
func WithTracer(tracer Tracer) Option {
	return func(c *Client) { c.tracer = tracer }
}

// RequestBuilder builds an individual HTTP request.
type RequestBuilder struct {
	mu          sync.Mutex
	client      *Client
	method      string
	url         string
	headers     http.Header
	queryParams url.Values
	body        io.Reader
	bodyBytes   []byte
	bodyErr     error
	auth        Auth
	timeout     time.Duration
	files       map[string]string
	formFields  map[string]string
	debug       bool
}

func (c *Client) newRequest(method, rawURL string) *RequestBuilder {
	headers := c.headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	params := make(url.Values, len(c.queryParams))
	for k, vv := range c.queryParams {
		params[k] = append([]string(nil), vv...)
	}
	return &RequestBuilder{
		client: c, method: method, url: rawURL, headers: headers,
		queryParams: params, auth: c.auth, timeout: c.timeout,
		files: make(map[string]string), formFields: make(map[string]string),
	}
}

// Standard HTTP method constructors.
func (c *Client) GET(url string) *RequestBuilder     { return c.newRequest(http.MethodGet, url) }
func (c *Client) POST(url string) *RequestBuilder    { return c.newRequest(http.MethodPost, url) }
func (c *Client) PUT(url string) *RequestBuilder     { return c.newRequest(http.MethodPut, url) }
func (c *Client) PATCH(url string) *RequestBuilder   { return c.newRequest(http.MethodPatch, url) }
func (c *Client) DELETE(url string) *RequestBuilder  { return c.newRequest(http.MethodDelete, url) }
func (c *Client) HEAD(url string) *RequestBuilder    { return c.newRequest(http.MethodHead, url) }
func (c *Client) OPTIONS(url string) *RequestBuilder { return c.newRequest(http.MethodOptions, url) }
func (c *Client) TRACE(url string) *RequestBuilder   { return c.newRequest(http.MethodTrace, url) }

// QUERY initiates a safe, idempotent server-side query. The query definition is
// typically sent in the request body, which should have an appropriate Content-Type.
// QUERY is defined by RFC 10008; servers and intermediaries must support it to work.
func (c *Client) QUERY(url string) *RequestBuilder { return c.newRequest("QUERY", url) }

// Header sets a request header.
func (rb *RequestBuilder) Header(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.headers.Set(key, value)
	return rb
}

// Headers sets multiple request headers.
func (rb *RequestBuilder) Headers(headers map[string]string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for k, v := range headers {
		rb.headers.Set(k, v)
	}
	return rb
}

// QueryParam appends a query parameter.
func (rb *RequestBuilder) QueryParam(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.queryParams.Add(key, value)
	return rb
}

// QueryParams sets query parameters, replacing values for matching keys.
func (rb *RequestBuilder) QueryParams(params map[string]string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for k, v := range params {
		rb.queryParams.Set(k, v)
	}
	return rb
}

// Body sets a raw request body.
func (rb *RequestBuilder) Body(body io.Reader) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.body = body
	return rb
}

// JSON encodes v as JSON. Encoding errors are returned by Send rather than hidden in the body.
func (rb *RequestBuilder) JSON(v any) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.headers.Set("Content-Type", "application/json")
	rb.bodyBytes, rb.bodyErr = json.Marshal(v)
	return rb
}

// File attaches a local file as a multipart/form-data upload.
func (rb *RequestBuilder) File(fieldName, filePath string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.files[fieldName] = filePath
	return rb
}

// Field adds a multipart text field.
func (rb *RequestBuilder) Field(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.formFields[key] = value
	return rb
}

// Auth overrides client-level authentication for this request.
func (rb *RequestBuilder) Auth(auth Auth) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.auth = auth
	return rb
}

// Timeout overrides the client-level timeout for this request.
func (rb *RequestBuilder) Timeout(timeout time.Duration) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.timeout = timeout
	return rb
}

// Debug enables trace output for this request. The report is logged after response headers
// arrive (or when the request fails); the response also exposes Debug() and Trace().
func (rb *RequestBuilder) Debug() *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.debug = true
	return rb
}

func (rb *RequestBuilder) buildURL() (string, error) {
	fullURL := rb.url
	if rb.client.baseURL != "" {
		base, err := url.Parse(rb.client.baseURL)
		if err != nil {
			return "", fmt.Errorf("httpx: invalid base URL: %w", err)
		}
		ref, err := url.Parse(rb.url)
		if err != nil {
			return "", fmt.Errorf("httpx: invalid request URL: %w", err)
		}
		fullURL = base.ResolveReference(ref).String()
	}
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("httpx: invalid URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("httpx: URL must be absolute or have a valid BaseURL: %q", fullURL)
	}
	q := u.Query()
	for k, vv := range rb.queryParams {
		for _, v := range vv {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (t *TraceInfo) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			t.dnsStart = time.Now()
			t.mu.Unlock()
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t.mu.Lock()
			if !t.dnsStart.IsZero() {
				t.DNSLookup += time.Since(t.dnsStart)
			}
			t.mu.Unlock()
		},
		ConnectStart: func(_, _ string) {
			t.mu.Lock()
			if t.connectStart.IsZero() {
				t.connectStart = time.Now()
			}
			t.mu.Unlock()
		},
		ConnectDone: func(_, _ string, _ error) {
			t.mu.Lock()
			if !t.connectStart.IsZero() && t.TCPConnTime == 0 {
				t.TCPConnTime = time.Since(t.connectStart)
			}
			t.mu.Unlock()
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			t.tlsStart = time.Now()
			t.mu.Unlock()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			t.mu.Lock()
			if !t.tlsStart.IsZero() {
				t.TLSHandshake += time.Since(t.tlsStart)
			}
			if !t.connectStart.IsZero() && t.ConnTime == 0 {
				t.ConnTime = time.Since(t.connectStart)
			}
			t.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			t.IsConnReused = info.Reused
			t.IsConnWasIdle = info.WasIdle
			t.ConnIdleTime = info.IdleTime
			if info.Conn != nil && info.Conn.RemoteAddr() != nil {
				t.RemoteAddr = info.Conn.RemoteAddr().String()
			}
			if !t.connectStart.IsZero() && t.ConnTime == 0 {
				t.ConnTime = time.Since(t.connectStart)
			}
			t.gotConnAt = time.Now()
			t.mu.Unlock()
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			t.mu.Lock()
			t.requestWritten = time.Now()
			t.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			t.firstByte = time.Now()
			if !t.requestWritten.IsZero() {
				t.ServerTime = t.firstByte.Sub(t.requestWritten)
			}
			t.mu.Unlock()
		},
	}
}

// Send executes the request and returns the response. The caller must close the response body.
func (rb *RequestBuilder) Send(ctx context.Context) (*Response, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if rb.bodyErr != nil {
		return nil, fmt.Errorf("httpx: failed to encode JSON body: %w", rb.bodyErr)
	}
	fullURL, err := rb.buildURL()
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if len(rb.files) > 0 || len(rb.formFields) > 0 {
		buf := &bytes.Buffer{}
		mw := multipart.NewWriter(buf)
		for field, path := range rb.files {
			file, openErr := os.Open(path)
			if openErr != nil {
				return nil, fmt.Errorf("httpx: failed to open file %q: %w", path, openErr)
			}
			part, partErr := mw.CreateFormFile(field, filepath.Base(path))
			if partErr == nil {
				_, partErr = io.Copy(part, file)
			}
			closeErr := file.Close()
			if partErr != nil {
				return nil, fmt.Errorf("httpx: failed to add file %q: %w", path, partErr)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("httpx: failed to close file %q: %w", path, closeErr)
			}
		}
		for k, v := range rb.formFields {
			if err := mw.WriteField(k, v); err != nil {
				return nil, fmt.Errorf("httpx: failed to write form field %q: %w", k, err)
			}
		}
		if err := mw.Close(); err != nil {
			return nil, fmt.Errorf("httpx: failed to finalize multipart body: %w", err)
		}
		bodyReader = buf
		rb.headers.Set("Content-Type", mw.FormDataContentType())
	} else if rb.bodyBytes != nil {
		bodyReader = bytes.NewReader(rb.bodyBytes)
	} else {
		bodyReader = rb.body
	}

	// Keep a per-request timeout alive until the response body is closed.
	// Canceling it when Send returns would make streaming/body reads fail.
	var cancel context.CancelFunc
	if rb.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, rb.timeout)
	}
	req, err := http.NewRequestWithContext(ctx, rb.method, fullURL, bodyReader)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, fmt.Errorf("httpx: failed to create request: %w", err)
	}
	req.Header = rb.headers.Clone()
	if rb.auth != nil {
		if err := rb.auth.Apply(req); err != nil {
			if cancel != nil {
				cancel()
			}
			return nil, fmt.Errorf("httpx: failed to apply auth: %w", err)
		}
	}
	if rb.client.tracer != nil {
		rb.client.tracer.Inject(ctx, req)
	}

	start := time.Now()
	trace := &TraceInfo{
		Method: req.Method, URL: req.URL.String(), Path: req.URL.EscapedPath(),
		RequestAttempt: 1, startedAt: start,
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace.hooks()))
	if rb.client.logger != nil {
		rb.client.logger.Info(ctx, "HTTP request started", "method", req.Method, "url", req.URL.String(), "path", req.URL.Path)
	}
	policy := rb.client.retryPolicy
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	methodRetryable := req.Method == http.MethodGet || req.Method == http.MethodHead || req.Method == http.MethodOptions || req.Method == http.MethodDelete
	if !methodRetryable || req.Body != nil && req.Body != http.NoBody {
		maxAttempts = 1
	}
	var resp *http.Response
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		trace.mu.Lock()
		trace.RequestAttempt = attempt
		trace.mu.Unlock()
		attemptReq := req
		if attempt > 1 {
			attemptReq = req.Clone(ctx)
		}
		if rb.client.requestHook != nil {
			rb.client.requestHook(ctx, attemptReq)
		}
		resp, err = rb.client.httpClient.Do(attemptReq)
		retry := false
		if err != nil {
			retry = policy.RetryNetworkErrors && attempt < maxAttempts
		}
		if err == nil && attempt < maxAttempts {
			for _, code := range policy.RetryableStatusCodes {
				if resp.StatusCode == code {
					retry = true
					break
				}
			}
		}
		if !retry {
			break
		}
		retryResponse := resp
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			resp = nil
		}
		delay := retryBackoff(policy, attempt, retryResponse, time.Now())
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				err = ctx.Err()
				attempt = maxAttempts
			case <-timer.C:
			}
			if err != nil {
				break
			}
		}
	}
	trace.mu.Lock()
	trace.TotalTime = time.Since(start)
	trace.ResponseTime = 0
	if !trace.firstByte.IsZero() {
		// Time between the first response byte and fully parsed response headers.
		trace.ResponseTime = time.Since(trace.firstByte)
	}
	if resp != nil {
		trace.StatusCode = resp.StatusCode
	}
	trace.mu.Unlock()

	if err != nil {
		if cancel != nil {
			cancel()
		}
		if rb.client.logger != nil {
			rb.client.logger.Error(ctx, "HTTP request failed", "method", req.Method, "url", req.URL.String(), "path", req.URL.Path, "error", err, "duration", time.Since(start))
			if rb.debug {
				rb.writeDebug(ctx, trace)
			}
		}
		return nil, fmt.Errorf("httpx: request execution failed: %w", err)
	}
	if rb.client.responseHook != nil {
		rb.client.responseHook(ctx, resp)
	}
	if cancel != nil && resp.Body != nil {
		resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	}
	wrapped := &Response{Response: resp, trace: trace, maxBodyBytes: rb.client.maxResponseBytes}
	if rb.client.logger != nil {
		rb.client.logger.Info(ctx, "HTTP request completed", "method", req.Method, "url", req.URL.String(), "path", req.URL.Path, "status_code", resp.StatusCode, "duration", time.Since(start))
		if rb.debug {
			rb.writeDebug(ctx, trace)
		}
	}
	return wrapped, nil
}

func (rb *RequestBuilder) writeDebug(ctx context.Context, trace *TraceInfo) {
	if rb.client.logger != nil {
		rb.client.logger.Debug(ctx, trace.String())
		return
	}
	fmt.Fprintln(os.Stderr, trace.String())
}

// cancelOnCloseBody keeps a request timeout context alive while callers consume
// the response body, then releases the context resources when the body is closed.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return err
}

// Close closes the body. It drains up to 4 KiB to improve keep-alive reuse without
// allowing an unexpectedly large response to block Close indefinitely.
func (r *Response) Close() error {
	if r != nil && r.Response != nil && r.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 4096))
		return r.Body.Close()
	}
	return nil
}

// CopyTo streams the response body to dst without buffering the entire response in memory.
// It closes the response body when finished.
func (r *Response) CopyTo(dst io.Writer) (int64, error) {
	if r == nil || r.Response == nil || r.Body == nil {
		return 0, fmt.Errorf("httpx: nil response")
	}
	defer r.Close()
	return io.Copy(dst, r.Body)
}

// IsSuccess reports whether the response status is 2xx.
func (r *Response) IsSuccess() bool {
	return r != nil && r.Response != nil && r.StatusCode >= 200 && r.StatusCode < 300
}

// ToJSON reads, closes, and decodes a successful response into T.
func ToJSON[T any](resp *Response, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, err
	}
	if resp == nil || resp.Response == nil || resp.Body == nil {
		return zero, fmt.Errorf("httpx: nil response")
	}
	defer resp.Close()
	if !resp.IsSuccess() {
		limit := int64(10 << 20)
		if resp.maxBodyBytes > 0 {
			limit = resp.maxBodyBytes
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if int64(len(body)) > limit {
			body = append(body[:limit], []byte("…[truncated]")...)
		}
		if readErr != nil {
			return zero, readErr
		}
		return zero, &HTTPError{StatusCode: resp.StatusCode, Status: resp.Status, Body: string(body), URL: resp.Request.URL.String()}
	}
	var target T
	reader := io.Reader(resp.Body)
	if resp.maxBodyBytes > 0 {
		reader = io.LimitReader(resp.Body, resp.maxBodyBytes+1)
	}
	limited := &countingReader{r: reader, limit: resp.maxBodyBytes}
	if err := json.NewDecoder(limited).Decode(&target); err != nil {
		return zero, fmt.Errorf("httpx: failed to decode response JSON: %w", err)
	}
	if resp.maxBodyBytes > 0 && limited.n > resp.maxBodyBytes {
		return zero, fmt.Errorf("httpx: response body exceeds %d bytes", resp.maxBodyBytes)
	}
	return target, nil
}

type countingReader struct {
	r        io.Reader
	n, limit int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// BearerAuth applies a bearer token.
type BearerAuth struct{ Token string }

func (b BearerAuth) Apply(req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+b.Token)
	return nil
}
func NewBearerAuth(token string) Auth { return BearerAuth{Token: token} }

// BasicAuth applies HTTP Basic authentication.
type BasicAuth struct{ Username, Password string }

func (b BasicAuth) Apply(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("httpx: cannot apply basic auth to a nil request")
	}
	req.SetBasicAuth(b.Username, b.Password)
	return nil
}

// NewBasicAuth creates an HTTP Basic authentication provider.
func NewBasicAuth(username, password string) Auth {
	return BasicAuth{Username: username, Password: password}
}

// APIKeyAuth sends an API key in a request header, for example X-API-Key.
// Header must be a non-empty header name and Key must be non-empty.
type APIKeyAuth struct {
	Header string
	Key    string
}

func (a APIKeyAuth) Apply(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("httpx: cannot apply API key auth to a nil request")
	}
	if a.Header == "" {
		return fmt.Errorf("httpx: API key header name must not be empty")
	}
	if a.Key == "" {
		return fmt.Errorf("httpx: API key must not be empty")
	}
	req.Header.Set(a.Header, a.Key)
	return nil
}

// NewAPIKeyAuth creates an API key authentication provider that sends key in header.
// Example: NewAPIKeyAuth("X-API-Key", "your-api-key").
func NewAPIKeyAuth(header, key string) Auth {
	return APIKeyAuth{Header: header, Key: key}
}

// APIKeyQueryAuth sends an API key as a URL query parameter. Prefer header-based
// API keys when the server supports them because URLs are commonly logged.
type APIKeyQueryAuth struct {
	Param string
	Key   string
}

func (a APIKeyQueryAuth) Apply(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("httpx: cannot apply API key query auth to a nil request")
	}
	if a.Param == "" {
		return fmt.Errorf("httpx: API key query parameter must not be empty")
	}
	if a.Key == "" {
		return fmt.Errorf("httpx: API key must not be empty")
	}
	q := req.URL.Query()
	q.Set(a.Param, a.Key)
	req.URL.RawQuery = q.Encode()
	return nil
}

// NewAPIKeyQueryAuth creates an API key authentication provider that sends key
// in a query parameter. Example: NewAPIKeyQueryAuth("api_key", "your-api-key").
func NewAPIKeyQueryAuth(param, key string) Auth {
	return APIKeyQueryAuth{Param: param, Key: key}
}
