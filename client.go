package httpx

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

// PoolConfig configures the standard HTTP transport connection pool.
type PoolConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
}

// Option configures a Client. Options only record intent; the HTTP client is built once
// at the end of New, so the order of options does not matter.
type Option func(*Client)

func defaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second, KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// New creates a client with connection pooling enabled by default.
func New(options ...Option) *Client {
	defaultHeaders := make(http.Header)
	defaultHeaders.Set("User-Agent", "httpx/"+Version)
	c := &Client{
		headers:          defaultHeaders,
		queryParams:      make(url.Values),
		timeout:          defaultTimeout,
		maxResponseBytes: defaultMaxResponse,
	}
	for _, opt := range options {
		if opt != nil {
			opt(c)
		}
	}
	c.build()
	return c
}

// build assembles the final *http.Client from everything the options recorded.
func (c *Client) build() {
	if c.rawBaseURL != "" {
		u, err := url.Parse(c.rawBaseURL)
		switch {
		case err != nil:
			c.initErr = fmt.Errorf("httpx: invalid base URL: %w", err)
		case u.Scheme == "" || u.Host == "":
			c.initErr = fmt.Errorf("httpx: base URL must be absolute: %q", c.rawBaseURL)
		default:
			c.baseURL = u
		}
	}

	// Never mutate a client the caller handed us.
	var hc http.Client
	if c.userClient != nil {
		hc = *c.userClient
		if !c.timeoutSet {
			c.timeout = c.userClient.Timeout
		}
	}

	rt := c.transport
	if rt == nil {
		rt = hc.Transport
	}
	if rt == nil {
		rt = defaultTransport()
	}

	needsTransportTweaks := c.pool != nil || c.tlsConfig != nil || c.proxySet
	if needsTransportTweaks {
		if t, ok := rt.(*http.Transport); ok && t != nil {
			t = t.Clone()
			if c.pool != nil {
				applyPool(t, *c.pool)
			}
			if c.tlsConfig != nil {
				t.TLSClientConfig = c.tlsConfig.Clone()
			}
			if c.proxySet {
				t.Proxy = c.proxy
			}
			rt = t
		} else if c.initErr == nil {
			c.initErr = errors.New("httpx: WithPoolConfig/WithTLSConfig/WithProxy require an *http.Transport")
		}
	}

	for i := len(c.middlewares) - 1; i >= 0; i-- {
		if c.middlewares[i] != nil {
			rt = c.middlewares[i](rt)
		}
	}
	hc.Transport = rt
	if c.checkRedirect != nil {
		hc.CheckRedirect = c.checkRedirect
	}
	if c.jar != nil {
		hc.Jar = c.jar
	}
	// The deadline is enforced with a per-request context instead, so a per-request
	// Timeout() can both shorten and extend it. http.Client.Timeout would silently win.
	hc.Timeout = 0
	c.httpClient = &hc
}

func applyPool(t *http.Transport, cfg PoolConfig) {
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
}

// Close releases idle connections held by the client's transport.
func (c *Client) Close() { c.httpClient.CloseIdleConnections() }

// WithPoolConfig configures connection pooling. Zero values retain the transport defaults.
// It requires the underlying transport to be an *http.Transport.
func WithPoolConfig(cfg PoolConfig) Option { return func(c *Client) { c.pool = &cfg } }

// WithTLSConfig sets the TLS client configuration (custom CAs, client certificates for mTLS, ...).
// It requires the underlying transport to be an *http.Transport.
func WithTLSConfig(cfg *tls.Config) Option { return func(c *Client) { c.tlsConfig = cfg } }

// WithProxy sets the proxy function. Pass nil to disable proxies entirely.
func WithProxy(proxy func(*http.Request) (*url.URL, error)) Option {
	return func(c *Client) { c.proxy, c.proxySet = proxy, true }
}

// WithMiddleware appends RoundTripper middlewares (tracing, metrics, rate limiting, ...).
func WithMiddleware(m ...Middleware) Option {
	return func(c *Client) { c.middlewares = append(c.middlewares, m...) }
}

// WithCheckRedirect sets the redirect policy. Return http.ErrUseLastResponse to stop following.
func WithCheckRedirect(fn func(req *http.Request, via []*http.Request) error) Option {
	return func(c *Client) { c.checkRedirect = fn }
}

// WithCookieJar sets the cookie jar.
func WithCookieJar(jar http.CookieJar) Option { return func(c *Client) { c.jar = jar } }

// WithMaxResponseBytes sets the maximum body size consumed by ToJSON, ToBytes and ToString.
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
func WithTransport(transport http.RoundTripper) Option {
	return func(c *Client) {
		if transport != nil {
			c.transport = transport
		}
	}
}

// WithHTTPClient uses a copy of a custom *http.Client; the original is never modified.
// Its Timeout becomes the default request timeout unless WithTimeout is also given.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) { c.userClient = client }
}

// WithTimeout sets the default timeout for requests. It covers the whole call, including
// all retry attempts, backoff waits and (until Close) reading the response body.
// Zero disables the timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) { c.timeout, c.timeoutSet = timeout, true }
}

// WithBaseURL sets a base URL prepended to relative request paths. Path segments of the
// base are preserved: base "https://x.com/v1" and path "/users" give "https://x.com/v1/users".
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.rawBaseURL = baseURL }
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
