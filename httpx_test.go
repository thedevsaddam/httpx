package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestToJSONAndHTTPError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"ok":true}`) }))
		defer srv.Close()
		resp, err := New().GET(srv.URL).Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got, err := ToJSON[struct {
			OK bool `json:"ok"`
		}](resp, nil)
		if err != nil || !got.OK {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
	t.Run("typed status error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "nope", http.StatusBadRequest) }))
		defer srv.Close()
		resp, err := New().GET(srv.URL).Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = ToJSON[map[string]any](resp, nil)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected typed HTTPError, got %v", err)
		}
	})
}

func TestRequestTimeoutDoesNotCancelBodyBeforeRead(t *testing.T) {
	payload := strings.Repeat("x", 64*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = io.WriteString(w, payload)
	}))
	defer srv.Close()

	resp, err := New(WithTimeout(2 * time.Second)).GET(srv.URL).Do(context.Background())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Close()
	if err != nil {
		t.Fatalf("read response body after Send returned: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("read %d bytes, want %d", len(got), len(payload))
	}
}

func TestResponseSizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"payload":"long enough"}`) }))
	defer srv.Close()
	resp, err := New(WithMaxResponseBytes(8)).GET(srv.URL).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = ToJSON[map[string]any](resp, nil)
	if err == nil {
		t.Fatal("expected response size limit error")
	}
}

func TestRetryPolicyAndHooks(t *testing.T) {
	var requests atomic.Int32
	var requestHooks atomic.Int32
	var responseHooks atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			attempt := requests.Add(1)

			if attempt < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("temporarily unavailable"))
				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	))
	defer server.Close()

	policy := RetryPolicy{
		MaxAttempts:          3,
		InitialBackoff:       time.Millisecond,
		MaxBackoff:           5 * time.Millisecond,
		RetryableStatusCodes: []int{http.StatusServiceUnavailable},
	}

	client := New(
		WithRetryPolicy(policy),
		WithRequestHook(func(ctx context.Context, req *http.Request) {
			requestHooks.Add(1)
		}),
		WithResponseHook(func(ctx context.Context, resp *http.Response) {
			responseHooks.Add(1)
		}),
	)

	resp, err := client.GET(server.URL).Do(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", string(body), "ok")
	}

	if got := requests.Load(); got != 3 {
		t.Errorf("requests = %d, want 3", got)
	}

	if got := requestHooks.Load(); got != 3 {
		t.Errorf("requestHooks = %d, want 3", got)
	}

	if got := responseHooks.Load(); got != 3 {
		t.Errorf("responseHooks = %d, want 3", got)
	}
}

func TestStandardRetryPolicyAndJitter(t *testing.T) {
	policy := StandardRetryPolicy()
	if policy.MaxAttempts != 3 ||
		!policy.Jitter ||
		!policy.RetryNetworkErrors ||
		!policy.RespectRetryAfter {
		t.Fatalf("unexpected standard retry policy: %+v", policy)
	}

	got, giveUp := retryDelay(policy, 1, nil, time.Now())
	if giveUp || got < 0 || got > policy.InitialBackoff {
		t.Fatalf(
			"first jittered backoff = %s, giveUp = %t; want [0, %s]",
			got, giveUp, policy.InitialBackoff,
		)
	}

	got, giveUp = retryDelay(policy, 4, nil, time.Now())
	if giveUp || got < 0 || got > policy.MaxBackoff {
		t.Fatalf(
			"capped jittered backoff = %s, giveUp = %t; want [0, %s]",
			got, giveUp, policy.MaxBackoff,
		)
	}
}

func TestRetryAfterHeader(t *testing.T) {
	policy := StandardRetryPolicy()
	resp := &http.Response{Header: make(http.Header)}

	resp.Header.Set("Retry-After", "1")
	got, giveUp := retryDelay(policy, 1, resp, time.Now())
	if giveUp || got != time.Second {
		t.Fatalf("Retry-After delay = %s, giveUp = %t; want 1s", got, giveUp)
	}

	resp.Header.Set("Retry-After", "10")
	got, giveUp = retryDelay(policy, 1, resp, time.Now())
	if !giveUp {
		t.Fatalf("Retry-After 10s should give up when MaxBackoff is %s; got delay %s", policy.MaxBackoff, got)
	}
}

func TestClientConcurrentRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"ok":true}`) }))
	defer srv.Close()
	client := New(WithPoolConfig(PoolConfig{MaxIdleConns: 32, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 16}))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.GET(srv.URL).Do(context.Background())
			if err != nil {
				t.Errorf("Send: %v", err)
				return
			}
			defer resp.Close()
			if !resp.IsSuccess() {
				t.Errorf("unexpected status: %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
}

func TestAuthenticationProviders(t *testing.T) {
	t.Run("bearer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com", nil)
		if err := NewBearerAuth("token-123").Apply(req); err != nil {
			t.Fatal(err)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("Authorization = %q", got)
		}
	})
	t.Run("basic", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com", nil)
		if err := NewBasicAuth("alice", "secret").Apply(req); err != nil {
			t.Fatal(err)
		}
		user, pass, ok := req.BasicAuth()
		if !ok || user != "alice" || pass != "secret" {
			t.Fatalf("BasicAuth() = (%q, %q, %t)", user, pass, ok)
		}
	})
	t.Run("API key header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com", nil)
		if err := NewAPIKeyAuth("X-API-Key", "key-123").Apply(req); err != nil {
			t.Fatal(err)
		}
		if got := req.Header.Get("X-API-Key"); got != "key-123" {
			t.Fatalf("X-API-Key = %q", got)
		}
	})
	t.Run("API key query", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com/items?page=2", nil)
		if err := NewAPIKeyQueryAuth("api_key", "key-123").Apply(req); err != nil {
			t.Fatal(err)
		}
		q := req.URL.Query()
		if q.Get("api_key") != "key-123" || q.Get("page") != "2" {
			t.Fatalf("query = %v", q)
		}
	})
	t.Run("reject empty API key header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "https://example.com", nil)
		if err := NewAPIKeyAuth("X-API-Key", "").Apply(req); err == nil {
			t.Fatal("expected error for empty API key")
		}
	})
}

func TestRetryHonorsParentContextCancellation(t *testing.T) {
	var requests atomic.Int32
	firstRequest := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) == 1 {
				close(firstRequest)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		},
	))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())

	client := New(
		WithTimeout(5*time.Second),
		WithRetryPolicy(RetryPolicy{
			MaxAttempts:          5,
			InitialBackoff:       500 * time.Millisecond,
			MaxBackoff:           time.Second,
			RetryableStatusCodes: []int{http.StatusServiceUnavailable},
		}),
	)

	done := make(chan error, 1)
	go func() {
		_, err := client.GET(server.URL).Do(ctx)
		done <- err
	}()

	select {
	case <-firstRequest:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for first request")
	}

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error after parent context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not stop after parent context cancellation")
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}

func TestRequestTimeoutWhileReadingBody(t *testing.T) {
	firstChunk := "first-chunk"
	secondChunk := "second-chunk"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, firstChunk)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		select {
		case <-r.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}

		_, _ = io.WriteString(w, secondChunk)
	}))
	defer srv.Close()

	client := New(WithTimeout(100 * time.Millisecond))

	resp, err := client.GET(srv.URL).Do(context.Background())
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Close()

	got, err := io.ReadAll(resp.Body)

	// The timeout should expire during the body read.
	if err == nil {
		t.Fatalf("expected body read to fail after timeout; got %q", got)
	}
}

func TestEachRetryGetsFreshTimeout(t *testing.T) {
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			attempt := requests.Add(1)

			if attempt == 1 {
				// Do not send response headers before the first
				// attempt's timeout expires.
				<-r.Context().Done()
				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
		},
	))
	defer srv.Close()

	client := New(
		WithTimeout(100*time.Millisecond),
		WithRetryPolicy(RetryPolicy{
			MaxAttempts:        2,
			InitialBackoff:     10 * time.Millisecond,
			MaxBackoff:         10 * time.Millisecond,
			RetryNetworkErrors: true,
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.GET(srv.URL).Do(ctx)
	if err != nil {
		t.Fatalf("Do: expected retry to succeed, got %v", err)
	}
	defer resp.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q, want %q", body, "ok")
	}

	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

func TestBuildURLMergesBaseAndRequestQueryParams(t *testing.T) {
	client := New(
		WithBaseURL("https://api.example.com/v1?tenant=42&lang=en"),
	)

	got, err := client.buildURL(state{
		url: "/users?page=2&limit=10",
	})
	if err != nil {
		t.Fatalf("buildURL() error = %v", err)
	}

	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}

	q := u.Query()
	for key, want := range map[string]string{
		"tenant": "42",
		"lang":   "en",
		"page":   "2",
		"limit":  "10",
	} {
		if got := q.Get(key); got != want {
			t.Errorf("query parameter %q = %q, want %q", key, got, want)
		}
	}
}

func TestBearerAuthRejectsNilRequest(t *testing.T) {
	if err := NewBearerAuth("token").Apply(nil); err == nil {
		t.Fatal("expected nil request error")
	}
}

func TestAPIKeyAuthRejectsNilRequest(t *testing.T) {
	req := (*http.Request)(nil)
	if err := NewAPIKeyAuth("X-API-Key", "abc").Apply(req); err == nil {
		t.Fatal("expected nil request error")
	}
}

func TestRequestBuilderRejectsConflictingBodyModes(t *testing.T) {
	client := New()
	req := client.POST("https://example.com")

	req.JSON(map[string]string{"a": "b"})
	req.Form(map[string]string{"x": "y"})

	if _, err := req.Do(context.Background()); err == nil {
		t.Fatal("expected conflicting body mode error")
	}
}

func TestJSONEncodeErrorSurfacesFromDo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, err := New().POST(srv.URL).JSON(make(chan int)).Do(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to encode JSON body") {
		t.Fatalf("expected JSON encoding error, got %v", err)
	}
}

func TestBuildURLRejectsInvalidPathParam(t *testing.T) {
	client := New()
	_, err := client.buildURL(state{
		url: "/users/{id}",
		pathParams: map[string]string{
			"id": ".",
		},
	})
	if err == nil {
		t.Fatal("expected invalid path parameter error")
	}
}

func TestFileReaderRejectsNilReaderAndEmptyFilename(t *testing.T) {
	client := New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// nil reader
	_, err := client.POST(srv.URL).FileReader("file", nil, "a.txt").Do(context.Background())
	if err == nil {
		t.Fatal("expected nil reader error")
	}

	// empty filename
	_, err = client.POST(srv.URL).FileReader("file", strings.NewReader("abc"), "").Do(context.Background())
	if err == nil {
		t.Fatal("expected empty filename error")
	}
}

func TestToJSONEmptyBodyAndMalformedJSON(t *testing.T) {
	t.Run("empty response body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		resp, err := New().GET(srv.URL).Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Close()

		_, err = ToJSON[map[string]any](resp, nil)
		if !errors.Is(err, ErrEmptyBody) {
			t.Fatalf("expected ErrEmptyBody, got %v", err)
		}
	})

	t.Run("malformed json", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "{bad json")
		}))
		defer srv.Close()

		resp, err := New().GET(srv.URL).Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Close()

		_, err = ToJSON[map[string]any](resp, nil)
		if err == nil {
			t.Fatal("expected malformed JSON error")
		}
	})
}

func TestRetryStopsWhenRetryAfterExceedsMaxBackoff(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     make(http.Header),
	}
	resp.Header.Set("Retry-After", "120")

	delay, giveUp := retryDelay(RetryPolicy{
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		RespectRetryAfter: true,
	}, 1, resp, time.Now())

	if !giveUp {
		t.Fatalf("expected giveUp=true when Retry-After exceeds MaxBackoff, got delay=%s", delay)
	}
}

// ErrroInto tests
type apiErr struct{ Code, Message string }

func errServer(status int, contentType, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header()["Content-Type"] = nil // no sniffed type unless we set one
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
}

func TestErrorInto(t *testing.T) {
	const ok = `{"code":"x","message":"m"}`
	msg := strings.Repeat("a", 9000)
	huge := `{"code":"big","message":"` + msg + `"}`
	want := apiErr{Code: "x", Message: "m"}
	cases := []struct {
		name, ct, body string
		noErrorInto    bool
		decoded        bool
		want           apiErr // what the target holds afterwards
	}{
		{"json", "application/json", ok, false, true, want},
		{"json charset, any case", "Application/JSON; charset=utf-8", ok, false, true, want},
		{"+json type", "application/problem+json", ok, false, true, want},
		{"missing content type", "", ok, false, true, want},
		{"body over 4 KiB decoded in full", "application/json", huge, false, true, apiErr{"big", msg}},
		{"text/html is skipped", "text/html", ok, false, false, apiErr{}},
		{"garbage content type is skipped", "///;;", ok, false, false, apiErr{}},
		{"invalid json", "application/json", `{"code":`, false, false, apiErr{}},
		{"empty body", "application/json", "", false, false, apiErr{}},
		{"no ErrorInto, nothing decoded", "application/json", ok, true, false, apiErr{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := errServer(422, tc.ct, tc.body)
			defer srv.Close()
			var e apiErr
			rb := New().GET(srv.URL)
			if !tc.noErrorInto {
				rb = rb.ErrorInto(&e)
			}
			_, err := ToBytes(rb.Do(context.Background()))

			var he *HTTPError
			if !errors.As(err, &he) || he.StatusCode != 422 {
				t.Fatalf("want *HTTPError 422, got %v", err)
			}
			if he.Decoded != tc.decoded || e != tc.want {
				t.Fatalf("Decoded=%v target=%.80q, want %v %.80q", he.Decoded, e, tc.decoded, tc.want)
			}
			body := tc.body // Body is always the raw text, cut to 4 KiB
			if len(body) > errorBodyExcerpt {
				body = body[:errorBodyExcerpt] + "…[truncated]"
			}
			if he.Body != body {
				t.Fatalf("Body = %.60q, want %.60q", he.Body, body)
			}
		})
	}
}

func TestErrorIntoSuccessLeavesTargetAlone(t *testing.T) {
	srv := errServer(200, "application/json", `{"code":"from-success"}`)
	defer srv.Close()
	e := apiErr{Code: "keep"}
	if _, err := ToString(New().GET(srv.URL).ErrorInto(&e).Do(context.Background())); err != nil || e.Code != "keep" {
		t.Fatalf("err=%v target=%+v", err, e)
	}
}

// A body cut off at the size limit must not be decoded. A truncated JSON number
// is still valid JSON (100 digits fit a float64), which an object would not be.
func TestErrorIntoSkipsTruncatedBody(t *testing.T) {
	srv := errServer(500, "application/json", strings.Repeat("1", 5000))
	defer srv.Close()
	var v any
	_, err := ToBytes(New(WithMaxResponseBytes(100)).GET(srv.URL).ErrorInto(&v).Do(context.Background()))
	var he *HTTPError
	if !errors.As(err, &he) || he.Decoded || v != nil {
		t.Fatalf("err=%v Decoded=%v v=%v", err, he != nil && he.Decoded, v)
	}
}

func TestErrorIntoTarget(t *testing.T) {
	srv := errServer(200, "application/json", `{}`)
	defer srv.Close()
	for _, bad := range []any{apiErr{}, (*apiErr)(nil), "x"} {
		if _, err := New().GET(srv.URL).ErrorInto(bad).Do(context.Background()); err == nil || !strings.Contains(err.Error(), "ErrorInto") {
			t.Fatalf("%T: want an ErrorInto error, got %v", bad, err)
		}
	}
	resp, err := New().GET(srv.URL).ErrorInto(nil).Do(context.Background()) // nil turns it off
	if err != nil {
		t.Fatal(err)
	}
	resp.Close()
}
