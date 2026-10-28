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

// PoolConfig tunes the connection pool of the default transport.
// Zero values leave the transport's own setting alone.
type PoolConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
}

// Option configures a Client. Options only record what was asked for and
// New builds the http.Client once at the end, so their order doesn't matter.
type Option func(*Client)

func defaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

// New creates a client. Connection pooling is on by default.
func New(options ...Option) *Client {
	headers := make(http.Header)
	headers.Set("User-Agent", "httpx/"+Version)
	c := &Client{
		headers:          headers,
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

// build puts the final *http.Client together from the recorded options.
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

	// work on a copy, the caller's client stays untouched
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

	if c.pool != nil || c.tlsConfig != nil || c.proxySet {
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
			c.initErr = errors.New("httpx: WithPoolConfig, WithTLSConfig and WithProxy need an *http.Transport")
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
	// Timeouts are handled with a per-request context so Timeout() on a request
	// can lengthen them too. http.Client.Timeout would always win otherwise.
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

// Close drops idle connections held by the transport.
func (c *Client) Close() { c.httpClient.CloseIdleConnections() }

// WithPoolConfig sets up connection pooling. Needs an *http.Transport underneath.
func WithPoolConfig(cfg PoolConfig) Option { return func(c *Client) { c.pool = &cfg } }

// WithTLSConfig sets the TLS config, e.g. custom CAs or client certs for mTLS.
// Needs an *http.Transport underneath.
func WithTLSConfig(cfg *tls.Config) Option { return func(c *Client) { c.tlsConfig = cfg } }

// WithProxy sets the proxy function. Pass nil to turn proxies off.
func WithProxy(proxy func(*http.Request) (*url.URL, error)) Option {
	return func(c *Client) { c.proxy, c.proxySet = proxy, true }
}

// WithMiddleware adds RoundTripper middlewares (metrics, rate limiting, ...).
func WithMiddleware(m ...Middleware) Option {
	return func(c *Client) { c.middlewares = append(c.middlewares, m...) }
}

// WithCheckRedirect sets the redirect policy. Return http.ErrUseLastResponse to stop following.
func WithCheckRedirect(fn func(req *http.Request, via []*http.Request) error) Option {
	return func(c *Client) { c.checkRedirect = fn }
}

func WithCookieJar(jar http.CookieJar) Option { return func(c *Client) { c.jar = jar } }

// WithMaxResponseBytes caps what ToJSON, ToBytes and ToString will read.
// A body you read yourself from Do is not limited.
func WithMaxResponseBytes(n int64) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxResponseBytes = n
		}
	}
}

// WithRetryPolicy turns on retries. Only idempotent methods with replayable bodies are retried.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(c *Client) {
		p.RetryableStatusCodes = append([]int(nil), p.RetryableStatusCodes...)
		c.retryPolicy = p
	}
}

func WithRequestHook(h RequestHook) Option   { return func(c *Client) { c.requestHook = h } }
func WithResponseHook(h ResponseHook) Option { return func(c *Client) { c.responseHook = h } }

// WithClientName sets the X-Client-Name header.
func WithClientName(value string) Option {
	return func(c *Client) { c.headers.Set("X-Client-Name", value) }
}

func WithTransport(transport http.RoundTripper) Option {
	return func(c *Client) {
		if transport != nil {
			c.transport = transport
		}
	}
}

// WithHTTPClient builds on a copy of client, the original isn't modified.
// Its Timeout is the default request timeout unless WithTimeout is also set.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) { c.userClient = client }
}

// WithTimeout sets the default timeout for a request attempt. Zero disables it.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) { c.timeout, c.timeoutSet = timeout, true }
}

// WithBaseURL sets the base for relative paths. Its own path is kept, so
// "https://x.com/v1" plus "/users" gives "https://x.com/v1/users".
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.rawBaseURL = baseURL }
}

func WithHeader(key, value string) Option {
	return func(c *Client) { c.headers.Set(key, value) }
}

func WithQueryParam(key, value string) Option {
	return func(c *Client) { c.queryParams.Add(key, value) }
}

func WithAuth(auth Auth) Option { return func(c *Client) { c.auth = auth } }

func WithLogger(logger Logger) Option { return func(c *Client) { c.logger = logger } }

func WithTracer(tracer Tracer) Option { return func(c *Client) { c.tracer = tracer } }
