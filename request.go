package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
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

type fileReaderPart struct {
	field       string
	filename    string
	contentType string
	reader      io.Reader
}

// RequestBuilder builds a single request. Don't share one between goroutines
// that send it at the same time.
type RequestBuilder struct {
	mu          sync.Mutex
	client      *Client
	method      string
	url         string
	pathParams  map[string]string
	headers     http.Header
	queryParams url.Values
	kind        bodyKind
	kindErr     error // first clash between body modes
	body        io.Reader
	bodyBytes   []byte
	bodyErr     error
	auth        Auth
	timeout     time.Duration
	files       []filePart // in call order, a field may repeat
	fileReaders []fileReaderPart
	formFields  url.Values
	debug       bool
	retryable   bool
	errTarget   any
}

// setKind records the body mode and keeps the first conflict, if any.
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
	return &RequestBuilder{
		client: c, method: method, url: rawURL, headers: headers,
		queryParams: cloneValues(c.queryParams), auth: c.auth, timeout: c.timeout,
		formFields: make(url.Values),
	}
}

func (c *Client) GET(url string) *RequestBuilder     { return c.newRequest(http.MethodGet, url) }
func (c *Client) POST(url string) *RequestBuilder    { return c.newRequest(http.MethodPost, url) }
func (c *Client) PUT(url string) *RequestBuilder     { return c.newRequest(http.MethodPut, url) }
func (c *Client) PATCH(url string) *RequestBuilder   { return c.newRequest(http.MethodPatch, url) }
func (c *Client) DELETE(url string) *RequestBuilder  { return c.newRequest(http.MethodDelete, url) }
func (c *Client) HEAD(url string) *RequestBuilder    { return c.newRequest(http.MethodHead, url) }
func (c *Client) OPTIONS(url string) *RequestBuilder { return c.newRequest(http.MethodOptions, url) }

// QUERY is the safe, idempotent QUERY method, normally with the query in the
// body. The server and any proxies in between have to support it.
func (c *Client) QUERY(url string) *RequestBuilder { return c.newRequest(methodQuery, url) }

func (rb *RequestBuilder) Header(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.headers.Set(key, value)
	return rb
}

func (rb *RequestBuilder) Headers(headers map[string]string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for k, v := range headers {
		rb.headers.Set(k, v)
	}
	return rb
}

// PathParam fills in "{name}" in the URL with the escaped value, e.g. "/users/{id}".
func (rb *RequestBuilder) PathParam(name, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.pathParams == nil {
		rb.pathParams = make(map[string]string)
	}
	rb.pathParams[name] = value
	return rb
}

// QueryParam adds a query parameter.
func (rb *RequestBuilder) QueryParam(key, value string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.queryParams.Add(key, value)
	return rb
}

// QueryParams sets query parameters, replacing existing values for the same keys.
func (rb *RequestBuilder) QueryParams(params map[string]string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for k, v := range params {
		rb.queryParams.Set(k, v)
	}
	return rb
}

// Body sets a raw body. For retries it has to be replayable, which
// *bytes.Buffer, *bytes.Reader and *strings.Reader are.
func (rb *RequestBuilder) Body(body io.Reader) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyRaw)
	rb.body = body
	return rb
}

// JSON encodes v as the request body without escaping <, > and &.
// An encoding error is returned from Do.
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

// Form sends URL-encoded form data, one value per key.
func (rb *RequestBuilder) Form(fields map[string]string) *RequestBuilder {
	values := make(url.Values, len(fields))
	for k, v := range fields {
		values.Set(k, v)
	}
	return rb.FormValues(values)
}

// FormValues is Form with several values allowed per key.
func (rb *RequestBuilder) FormValues(values url.Values) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyForm)
	rb.bodyBytes = []byte(values.Encode())
	rb.bodyErr = nil
	rb.headers.Set(headerContentType, contentTypeForm)
	return rb
}

// File attaches a local file to a multipart upload. Call it as often as you
// like, even with the same field name; parts go out in call order.
func (rb *RequestBuilder) File(fieldName, filePath string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	rb.files = append(rb.files, filePart{field: fieldName, path: filePath})
	return rb
}

// FileReader attaches the content of r as a multipart file part. The content
// type is optional and falls back to the filename's extension.
//
// r is read once, when Do builds the body, and the result is buffered in
// memory so retries can replay it. You still own r and should close it. Since
// r is used up, sending the same builder twice needs a fresh reader.
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

// Fields adds several multipart text fields.
func (rb *RequestBuilder) Fields(fields url.Values) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.setKind(bodyMultipart)
	for key, values := range fields {
		for _, v := range values {
			rb.formFields.Add(key, v)
		}
	}
	return rb
}

// Auth overrides the client's auth for this request.
func (rb *RequestBuilder) Auth(auth Auth) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.auth = auth
	return rb
}

// Retryable lets a non-idempotent request (a POST, say) use the retry policy.
// Only do that when repeating it is harmless, ideally with an idempotency key,
// and the body can be replayed.
func (rb *RequestBuilder) Retryable() *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.retryable = true
	return rb
}

// IdempotencyKey sets the Idempotency-Key header and marks the request
// retryable, so POST and PATCH can be retried against servers that honor it.
func (rb *RequestBuilder) IdempotencyKey(key string) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.headers.Set("Idempotency-Key", key)
	rb.retryable = true
	return rb
}

// ErrorInto sets where the JSON body of a non-2xx response is decoded, so
// ToJSON, ToBytes and ToString fill it in and still return an *HTTPError:
//
//	var apiErr APIError
//	todo, err := httpx.ToJSON[Todo](client.GET(u).ErrorInto(&apiErr).Do(ctx))
//
// Only responses with a JSON Content-Type (application/json or a +json type)
// are decoded; a missing Content-Type counts as JSON. HTML, plain text and
// other types are left alone and stay available in HTTPError.Body.
//
// v has to be a non-nil pointer. With it set, the whole error body is read
// (up to the client's max response size) for decoding; HTTPError.Body still
// holds only a short excerpt, and HTTPError.Decoded says whether decoding worked.
func (rb *RequestBuilder) ErrorInto(v any) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.errTarget = v
	return rb
}

// Timeout overrides the client timeout for this request. It's per attempt;
// the context passed to Do still limits the whole call.
func (rb *RequestBuilder) Timeout(timeout time.Duration) *RequestBuilder {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.timeout = timeout
	return rb
}

// Debug prints the trace once the headers arrive or the request fails, to the
// logger if there is one and to stderr otherwise.
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

// Do sends the request. The caller has to close the response body.
//
// The timeout applies to each attempt on its own, so a 10s timeout gives every
// retry a fresh 10s, and on the last attempt it keeps running until the body
// is closed. ctx bounds the whole call, backoff waits included.
func (rb *RequestBuilder) Do(ctx context.Context) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c := rb.client
	if c.initErr != nil {
		return nil, c.initErr
	}
	s := rb.snapshot()

	if s.kindErr != nil {
		return nil, s.kindErr
	}
	if s.bodyErr != nil {
		return nil, fmt.Errorf("httpx: failed to encode JSON body: %w", s.bodyErr)
	}
	if s.errTarget != nil {
		if rv := reflect.ValueOf(s.errTarget); rv.Kind() != reflect.Pointer || rv.IsNil() {
			return nil, errors.New("httpx: ErrorInto needs a non-nil pointer")
		}
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

	// the per-attempt timeout is added later, in doWithRetry
	req, err := http.NewRequestWithContext(ctx, s.method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("httpx: failed to create request: %w", err)
	}
	req.Header = s.headers
	if s.auth != nil {
		if err := s.auth.Apply(req); err != nil {
			return nil, fmt.Errorf("httpx: failed to apply auth: %w", err)
		}
	}
	if c.tracer != nil {
		c.tracer.Inject(ctx, req)
	}

	// the real URL can carry secrets in its query, so only log this one
	safeURL := redactURL(req.URL)
	start := time.Now()
	trace := &TraceInfo{
		Method: req.Method, URL: safeURL, Path: req.URL.EscapedPath(),
		RequestAttempt: 1,
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace.hooks()))
	if c.logger != nil {
		c.logger.Debug(ctx, "HTTP request started", "method", req.Method, "url", safeURL)
	}

	// cancel belongs to the last attempt's timeout context, nil without a timeout.
	// On error doWithRetry has already called it.
	resp, cancel, err := rb.doWithRetry(ctx, req, s, trace)

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
		// *url.Error carries the raw URL with its query string, swap it for the safe one
		var ue *url.Error
		if errors.As(err, &ue) {
			ue.URL = safeURL
		}
		if c.logger != nil {
			c.logger.Error(ctx, "HTTP request failed", "method", req.Method, "url", safeURL, "error", err, "duration", time.Since(start))
		}
		if s.debug {
			rb.writeDebug(ctx, trace)
		}
		return nil, fmt.Errorf("httpx: request execution failed: %w", err)
	}

	if cancel != nil {
		switch {
		case resp.Body == nil:
			cancel()
		case resp.StatusCode == http.StatusSwitchingProtocols:
			// The caller owns the upgraded connection now, and it's writable
			// (WebSocket etc.), so don't wrap the body or cancel. The timer
			// cleans itself up at the deadline.
		default:
			resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
		}
	}
	if c.logger != nil {
		c.logger.Info(ctx, "HTTP request completed", "method", req.Method, "url", safeURL, "status_code", resp.StatusCode, "duration", time.Since(start))
	}
	if s.debug {
		rb.writeDebug(ctx, trace)
	}
	return &Response{Response: resp, trace: trace, maxBodyBytes: c.maxResponseBytes, method: req.Method, safeURL: safeURL, errTarget: s.errTarget}, nil
}

// doWithRetry is the attempt loop, each attempt with its own timeout. On error
// the response and cancel func are nil and the attempt context is already
// cancelled. On success the cancel func goes with the response and must be
// called when its body is closed.
func (rb *RequestBuilder) doWithRetry(ctx context.Context, req *http.Request, s state, trace *TraceInfo) (*http.Response, context.CancelFunc, error) {
	c := rb.client
	policy := c.retryPolicy
	maxAttempts := policy.MaxAttempts
	if maxAttempts < 1 || !methodRetryable(req.Method, s.retryable) {
		maxAttempts = 1
	}
	// can't retry a body that net/http isn't able to recreate
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		maxAttempts = 1
	}

	var lastErr error

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		trace.mu.Lock()
		trace.RequestAttempt = attempt
		trace.mu.Unlock()

		attemptCtx := req.Context()
		var cancel context.CancelFunc
		if s.timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(attemptCtx, s.timeout)
		}
		cancelAttempt := func() {
			if cancel != nil {
				cancel()
			}
		}

		attemptReq := req.Clone(attemptCtx)
		if attempt > 1 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				cancelAttempt()
				return nil, nil, fmt.Errorf("httpx: failed to recreate request body for retry: %w", err)
			}
			attemptReq.Body = body
		}
		if c.requestHook != nil {
			c.requestHook(ctx, attemptReq)
		}
		resp, err := c.httpClient.Do(attemptReq)

		if ctx.Err() != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			cancelAttempt()
			return nil, nil, ctx.Err()
		}

		if resp != nil && c.responseHook != nil {
			c.responseHook(ctx, resp)
		}

		if attempt >= maxAttempts {
			if err != nil {
				cancelAttempt()
				return nil, nil, err
			}
			return resp, cancel, nil
		}

		var delay time.Duration
		if err != nil {
			cancelAttempt()
			if !policy.RetryNetworkErrors || !retryableNetErr(err) {
				return nil, nil, err
			}
			// keep the failure for the ctx-deadline message, minus the query string
			var ue *url.Error
			if errors.As(err, &ue) {
				cp := *ue
				cp.URL = redactURL(req.URL)
				lastErr = &cp
			} else {
				lastErr = err
			}
			delay, _ = retryDelay(policy, attempt, nil, time.Now())
		} else {
			if !containsInt(policy.RetryableStatusCodes, resp.StatusCode) {
				return resp, cancel, nil
			}
			var giveUp bool
			delay, giveUp = retryDelay(policy, attempt, resp, time.Now())
			if giveUp {
				return resp, cancel, nil
			}
			lastErr = fmt.Errorf("last attempt returned status %d", resp.StatusCode)
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
			_ = resp.Body.Close()
			cancelAttempt()
		}

		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, nil, ctxError(ctx, lastErr)
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return nil, nil, ctxError(ctx, lastErr)
		}
	}
}

// ctxError returns the context's error, with the last failed attempt attached when there was one.
func ctxError(ctx context.Context, lastErr error) error {
	if lastErr != nil {
		return fmt.Errorf("%w (last attempt: %v)", ctx.Err(), lastErr)
	}
	return ctx.Err()
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

// redactURL drops the query string and user info, both of which tend to hold secrets.
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	r := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}
	return r.String()
}

// state is a copy of the builder taken under its lock, so Do never holds the
// lock while it does network I/O or sleeps between retries.
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
	errTarget   any
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
		debug:       rb.debug, retryable: rb.retryable, errTarget: rb.errTarget,
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
	if len(s.pathParams) > 0 {
		pairs := make([]string, 0, len(s.pathParams)*2)
		for name, value := range s.pathParams {
			if value == "." || value == ".." {
				return "", fmt.Errorf("httpx: invalid path parameter value %q", value)
			}
			pairs = append(pairs, "{"+name+"}", url.PathEscape(value))
		}
		raw = strings.NewReplacer(pairs...).Replace(raw)
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
		joined.RawPath = strings.TrimRight(c.baseURL.EscapedPath(), "/") + "/" + strings.TrimLeft(ref.EscapedPath(), "/")
		q := joined.Query()
		for key, values := range ref.Query() {
			for _, v := range values {
				q.Add(key, v)
			}
		}
		joined.RawQuery = q.Encode()
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
