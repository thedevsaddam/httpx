package httpx

import (
	"context"
	"crypto/x509"
	"errors"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

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

// NoRetryPolicy disables retries.
func NoRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 1,
	}
}

// FastRetryPolicy is suitable for fast internal API calls.
func FastRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          2,
		InitialBackoff:       50 * time.Millisecond,
		MaxBackoff:           200 * time.Millisecond,
		RetryableStatusCodes: []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    false,
	}
}

// AggressiveRetryPolicy retries transient failures more often.
func AggressiveRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          5,
		InitialBackoff:       200 * time.Millisecond,
		MaxBackoff:           5 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// BackgroundRetryPolicy is suitable for non-interactive jobs.
func BackgroundRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          8,
		InitialBackoff:       time.Second,
		MaxBackoff:           30 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// RateLimitRetryPolicy prioritizes handling HTTP 429 responses.
func RateLimitRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          4,
		InitialBackoff:       time.Second,
		MaxBackoff:           30 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusServiceUnavailable},
		RetryNetworkErrors:   false,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// ResilientRetryPolicy is intended for unreliable external APIs.
func ResilientRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          5,
		InitialBackoff:       500 * time.Millisecond,
		MaxBackoff:           10 * time.Second,
		RetryableStatusCodes: []int{429, 500, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// retryDelay returns how long to wait before the next attempt. giveUp is true when the
// server's Retry-After exceeds what the policy is willing to wait.
func retryDelay(policy RetryPolicy, retryNumber int, resp *http.Response, now time.Time) (delay time.Duration, giveUp bool) {
	if policy.RespectRetryAfter && resp != nil {
		if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
			var serverDelay time.Duration
			if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
				if seconds > int64(time.Duration(1<<63-1)/time.Second) {
					serverDelay = time.Duration(1<<63 - 1)
				} else {
					serverDelay = time.Duration(seconds) * time.Second
				}
			} else if when, err := http.ParseTime(value); err == nil && when.After(now) {
				serverDelay = when.Sub(now)
			}
			if serverDelay > 0 {
				if policy.MaxBackoff > 0 && serverDelay > policy.MaxBackoff {
					return 0, true
				}
				return serverDelay, false
			}
		}
	}
	capDelay := policy.InitialBackoff
	if capDelay <= 0 {
		return 0, false
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
	if policy.Jitter && capDelay > 0 {
		return time.Duration(rand.Int63n(int64(capDelay))), false
	}
	return capDelay, false
}

// retryableNetErr reports whether a transport error is worth retrying. Cancellation and
// certificate problems are permanent, so they are never retried.
func retryableNetErr(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var (
		unknownAuth x509.UnknownAuthorityError
		certInvalid x509.CertificateInvalidError
		hostErr     x509.HostnameError
	)
	if errors.As(err, &unknownAuth) || errors.As(err, &certInvalid) || errors.As(err, &hostErr) {
		return false
	}
	return true
}
