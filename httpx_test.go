package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
