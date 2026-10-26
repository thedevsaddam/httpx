package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type bodyKind int

const (
	bodyNone bodyKind = iota
	bodyRaw
	bodyJSON
	bodyForm
	bodyMultipart
)

func (k bodyKind) String() string {
	return [...]string{"none", "Body", "JSON", "Form", "File/FileReader/Field"}[k]
}

type filePart struct {
	field string
	path  string
}

// fileReaderPart is a multipart file part whose content comes from an io.Reader.
type fileReaderPart struct {
	field       string
	filename    string
	contentType string
	reader      io.Reader
}

// RequestBuilder builds an individual HTTP request.
type RequestBuilder struct {
	mu          sync.Mutex
	client      *Client
	method      string
	url         string
	pathParams  map[string]string
	headers     http.Header
	queryParams url.Values
	kind        bodyKind
	kindErr     error // first conflict between body modes
	body        io.Reader
	bodyBytes   []byte
	bodyErr     error
	auth        Auth
	timeout     time.Duration
	files       []filePart // ordered; the same field may repeat
	fileReaders []fileReaderPart
	formFields  url.Values
	debug       bool
	retryable   bool
}

// setKind records the body mode and remembers the first conflicting use.
func (rb *RequestBuilder) setKind(k bodyKind) {
	if rb.kind != bodyNone && rb.kind != k && rb.kindErr == nil {
		rb.kindErr = fmt.Errorf("httpx: conflicting request body modes: %s and %s", rb.kind, k)
	}
	rb.kind = k
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
		formFields: make(url.Values),
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

// QUERY initiates a safe, idempotent server-side query. The query definition is
// typically sent in the request body, which should have an appropriate Content-Type.
// Servers and intermediaries must support the QUERY method for this to work.
func (c *Client) QUERY(url string) *RequestBuilder { return c.newRequest(methodQuery, url) }

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

// PathParam replaces "{name}" in the URL with the escaped value, e.g. "/users/{id}".
func (rb *RequestBuilder) PathParam(name, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.pathParams == nil {
		rb.pathParams = make(map[string]string)
	}
	rb.pathParams[name] = value
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

// Body sets a raw request body. Retries need a replayable body: *bytes.Buffer,
// *bytes.Reader and *strings.Reader qualify.
func (rb *RequestBuilder) Body(body io.Reader) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyRaw)
	rb.body = body
	return rb
}

// JSON encodes v as JSON. Encoding errors are returned by Send rather than hidden in the body.
// HTML characters (<, >, &) are not escaped.
func (rb *RequestBuilder) JSON(v any) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyJSON)
	rb.headers.Set(headerContentType, contentTypeJSON)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		rb.bodyBytes, rb.bodyErr = nil, err
		return rb
	}
	rb.bodyBytes = bytes.TrimRight(buf.Bytes(), "\n")
	rb.bodyErr = nil
	return rb
}

// Form sets URL-encoded form data with one value per key.
func (rb *RequestBuilder) Form(fields map[string]string) *RequestBuilder {
	values := make(url.Values, len(fields))
	for key, value := range fields {
		values.Set(key, value)
	}
	return rb.FormValues(values)
}

// FormValues sets URL-encoded form data. It supports multiple values for the same key.
func (rb *RequestBuilder) FormValues(values url.Values) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyForm)
	rb.bodyBytes = []byte(values.Encode())
	rb.bodyErr = nil
	rb.headers.Set(headerContentType, contentTypeForm)
	return rb
}

// File attaches a local file as a multipart/form-data upload. It can be called several
// times, including with the same field name; parts are sent in call order.
func (rb *RequestBuilder) File(fieldName, filePath string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	rb.files = append(rb.files, filePart{field: fieldName, path: filePath})
	return rb
}

// FileReader attaches content from r as a multipart/form-data file part named fieldName,
// using filename as the uploaded file name. It can be called multiple times and combined
// with File and Field.
//
// The reader is consumed once, when Send builds the multipart body; the body is buffered
// in memory so retries can replay it. The caller keeps ownership of r and must close it
// if it is an io.Closer. Because r is consumed, a builder with FileReader cannot be
// Sent a second time unless the reader is reset by the caller.
func (rb *RequestBuilder) FileReader(fieldName string, r io.Reader, filename string, contentType ...string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	mimeType := ""
	if len(contentType) > 0 {
		mimeType = contentType[0]
	}
	if mimeType == "" {
		mimeType = mimeFor(filename)
	}
	rb.fileReaders = append(rb.fileReaders, fileReaderPart{field: fieldName, filename: filename, reader: r, contentType: mimeType})
	return rb
}

func mimeFor(name string) string {
	if t := mime.TypeByExtension(strings.ToLower(filepath.Ext(name))); t != "" {
		return t
	}
	return contentTypeOctet
}

// Field adds a multipart text field.
func (rb *RequestBuilder) Field(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	rb.formFields.Add(key, value)
	return rb
}

// Fields adds multiple multipart text field values.
func (rb *RequestBuilder) Fields(fields url.Values) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	for key, values := range fields {
		for _, value := range values {
			rb.formFields.Add(key, value)
		}
	}
	return rb
}

// Auth overrides client-level authentication for this request.
func (rb *RequestBuilder) Auth(auth Auth) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.auth = auth
	return rb
}

// Retryable explicitly allows this request to be retried when the configured RetryPolicy
// matches a retryable network error or status code. Use only when repeating the operation
// is safe, typically with a server-supported idempotency key. Request bodies must be replayable.
func (rb *RequestBuilder) Retryable() *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.retryable = true
	return rb
}

// IdempotencyKey sets the Idempotency-Key header and marks the request Retryable, which
// makes retrying POST and PATCH safe against servers that honor the key.
func (rb *RequestBuilder) IdempotencyKey(key string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.headers.Set("Idempotency-Key", key)
	rb.retryable = true
	return rb
}

// Timeout overrides the client-level timeout for this request. It covers all retry attempts.
func (rb *RequestBuilder) Timeout(timeout time.Duration) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.timeout = timeout
	return rb
}

// Debug enables trace output for this request. The report is logged after response headers
// arrive (or when the request fails), to the logger or to stderr when none is configured.
func (rb *RequestBuilder) Debug() *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.debug = true
	return rb
}

func methodRetryable(method string, explicit bool) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodDelete, http.MethodPut, methodQuery:
		return true
	}
	return explicit
}

// Send executes the request and returns the response. The caller must close the response body.
// The request timeout covers every attempt, and stays active until the body is closed.
func (rb *RequestBuilder) Do(ctx context.Context) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c := rb.client
	if c.initErr != nil {
		return nil, c.initErr
	}
	s := rb.snapshot() // lock is not held past this point

	if s.kindErr != nil {
		return nil, s.kindErr
	}
	if s.bodyErr != nil {
		return nil, fmt.Errorf("httpx: failed to encode JSON body: %w", s.bodyErr)
	}
	fullURL, err := c.buildURL(s)
	if err != nil {
		return nil, err
	}
	bodyReader, multipartCT, err := buildBody(s)
	if err != nil {
		return nil, err
	}
	if multipartCT != "" {
		s.headers.Set(headerContentType, multipartCT)
	}

	// Keep the per-request timeout alive until the response body is closed.
	var cancel context.CancelFunc
	if s.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	fail := func(err error) (*Response, error) {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, s.method, fullURL, bodyReader)
	if err != nil {
		return fail(fmt.Errorf("httpx: failed to create request: %w", err))
	}
	req.Header = s.headers
	if s.auth != nil {
		if err := s.auth.Apply(req); err != nil {
			return fail(fmt.Errorf("httpx: failed to apply auth: %w", err))
		}
	}
	if c.tracer != nil {
		c.tracer.Inject(ctx, req)
	}

	safeURL := redactURL(req.URL) // never log the raw URL: auth or callers may put secrets in the query
	start := time.Now()
	trace := &TraceInfo{
		Method: req.Method, URL: safeURL, Path: req.URL.EscapedPath(),
		RequestAttempt: 1, startedAt: start,
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace.hooks()))
	if c.logger != nil {
		c.logger.Debug(ctx, "HTTP request started", "method", req.Method, "url", safeURL)
	}

	resp, err := rb.doWithRetry(ctx, req, s, trace)

	trace.mu.Lock()
	trace.TotalTime = time.Since(start)
	if !trace.firstByte.IsZero() {
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
		if c.logger != nil {
			c.logger.Error(ctx, "HTTP request failed", "method", req.Method, "url", safeURL, "error", err, "duration", time.Since(start))
		}
		if s.debug {
			rb.writeDebug(ctx, trace)
		}
		return nil, fmt.Errorf("httpx: request execution failed: %w", err)
	}
	// A 101 body is a writable stream (WebSocket etc.); wrapping it would hide io.Writer.
	if cancel != nil && resp.Body != nil {
		if resp.StatusCode == http.StatusSwitchingProtocols {
			// The caller owns the connection now; the context must not outlive the call.
			// Not cancelling here: cancelling would tear the upgraded connection down.
			_ = cancel
		} else {
			resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
		}
	}
	if c.logger != nil {
		c.logger.Info(ctx, "HTTP request completed", "method", req.Method, "url", safeURL, "status_code", resp.StatusCode, "duration", time.Since(start))
	}
	if s.debug {
		rb.writeDebug(ctx, trace)
	}
	return &Response{Response: resp, trace: trace, maxBodyBytes: c.maxResponseBytes, method: req.Method, safeURL: safeURL}, nil
}

// doWithRetry runs the attempt loop. On a non-nil error the returned response is nil.
func (rb *RequestBuilder) doWithRetry(ctx context.Context, req *http.Request, s state, trace *TraceInfo) (*http.Response, error) {
	c := rb.client
	policy := c.retryPolicy
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 || !methodRetryable(req.Method, s.retryable) {
		maxAttempts = 1
	}
	// A body is retryable only when net/http can recreate it for each attempt.
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		maxAttempts = 1
	}

	for attempt := 1; ; attempt++ {
		trace.mu.Lock()
		trace.RequestAttempt = attempt
		trace.mu.Unlock()

		attemptReq := req
		if attempt > 1 {
			// Clone with the request's own context so the httptrace hooks survive.
			attemptReq = req.Clone(req.Context())
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, fmt.Errorf("httpx: failed to recreate request body for retry: %w", err)
				}
				attemptReq.Body = body
			}
		}
		if c.requestHook != nil {
			c.requestHook(ctx, attemptReq)
		}
		resp, err := c.httpClient.Do(attemptReq)
		if resp != nil && c.responseHook != nil {
			c.responseHook(ctx, resp)
		}

		if attempt >= maxAttempts {
			return resp, err
		}
		var delay time.Duration
		if err != nil {
			if ctx.Err() != nil || !policy.RetryNetworkErrors || !retryableNetErr(err) {
				return nil, err
			}
			delay, _ = retryDelay(policy, attempt, nil, time.Now())
		} else {
			if !containsInt(policy.RetryableStatusCodes, resp.StatusCode) {
				return resp, nil
			}
			var giveUp bool
			delay, giveUp = retryDelay(policy, attempt, resp, time.Now())
			if giveUp {
				return resp, nil // server wants a longer pause than we allow: hand the response back
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
			_ = resp.Body.Close()
		}

		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (rb *RequestBuilder) writeDebug(ctx context.Context, trace *TraceInfo) {
	if rb.client.logger != nil {
		rb.client.logger.Debug(ctx, trace.String())
		return
	}
	fmt.Fprintln(os.Stderr, trace.String())
}

// redactURL removes the query string and user info, which commonly carry secrets.
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	r := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}
	return r.String()
}

// state is a snapshot of the builder taken under its lock, so Send never holds the lock
// during network I/O or backoff sleeps.
type state struct {
	method      string
	url         string
	pathParams  map[string]string
	headers     http.Header
	queryParams url.Values
	kind        bodyKind
	kindErr     error
	body        io.Reader
	bodyBytes   []byte
	bodyErr     error
	auth        Auth
	timeout     time.Duration
	files       []filePart
	fileReaders []fileReaderPart
	formFields  url.Values
	debug       bool
	retryable   bool
}

func (rb *RequestBuilder) snapshot() state {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	pp := make(map[string]string, len(rb.pathParams))
	for k, v := range rb.pathParams {
		pp[k] = v
	}
	return state{
		method: rb.method, url: rb.url, pathParams: pp,
		headers: rb.headers.Clone(), queryParams: cloneValues(rb.queryParams),
		kind: rb.kind, kindErr: rb.kindErr, body: rb.body,
		bodyBytes: rb.bodyBytes, bodyErr: rb.bodyErr, auth: rb.auth, timeout: rb.timeout,
		files:       append([]filePart(nil), rb.files...),
		fileReaders: append([]fileReaderPart(nil), rb.fileReaders...),
		formFields:  cloneValues(rb.formFields),
		debug:       rb.debug, retryable: rb.retryable,
	}
}

func cloneValues(v url.Values) url.Values {
	out := make(url.Values, len(v))
	for k, vv := range v {
		out[k] = append([]string(nil), vv...)
	}
	return out
}

func (c *Client) buildURL(s state) (string, error) {
	raw := s.url
	for name, value := range s.pathParams {
		raw = strings.ReplaceAll(raw, "{"+name+"}", url.PathEscape(value))
	}
	ref, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("httpx: invalid request URL: %w", err)
	}
	var u *url.URL
	switch {
	case ref.Scheme != "" && ref.Host != "":
		u = ref
	case c.baseURL != nil:
		joined := *c.baseURL
		joined.Path = strings.TrimRight(c.baseURL.Path, "/") + "/" + strings.TrimLeft(ref.Path, "/")
		joined.RawPath = ""
		joined.RawQuery = ref.RawQuery
		joined.Fragment = ref.Fragment
		u = &joined
	default:
		return "", fmt.Errorf("httpx: URL must be absolute or have a valid BaseURL: %q", s.url)
	}
	q := u.Query()
	for k, vv := range s.queryParams {
		for _, v := range vv {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
