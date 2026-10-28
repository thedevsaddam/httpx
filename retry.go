package httpx

import (
	"context"
	"crypto/x509"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxDuration = time.Duration(math.MaxInt64)

// RetryPolicy controls retries. MaxAttempts counts the first try, so anything
// below 2 means no retries. With Jitter each backoff is a random value between
// zero and the exponential cap. With RespectRetryAfter, a server asking for
// more than MaxBackoff (when set) is not waited for: the response is handed
// back to the caller instead.
type RetryPolicy struct {
	MaxAttempts          int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	RetryableStatusCodes []int
	RetryNetworkErrors   bool
	Jitter               bool
	RespectRetryAfter    bool
}

// StandardRetryPolicy is a sane default: three attempts, network errors plus
// 429/502/503/504, jittered backoff capped at 2s, and Retry-After honored.
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

// NoRetryPolicy turns retries off.
func NoRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// FastRetryPolicy is meant for quick internal calls.
func FastRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          2,
		InitialBackoff:       50 * time.Millisecond,
		MaxBackoff:           200 * time.Millisecond,
		RetryableStatusCodes: []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
	}
}

// AggressiveRetryPolicy retries more often, and on 500s too.
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

// BackgroundRetryPolicy is for batch jobs where nobody is waiting on the result.
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

// RateLimitRetryPolicy is tuned for 429s.
func RateLimitRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          4,
		InitialBackoff:       time.Second,
		MaxBackoff:           30 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusServiceUnavailable},
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// ResilientRetryPolicy is for flaky third-party APIs.
func ResilientRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:          5,
		InitialBackoff:       500 * time.Millisecond,
		MaxBackoff:           10 * time.Second,
		RetryableStatusCodes: []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		RetryNetworkErrors:   true,
		Jitter:               true,
		RespectRetryAfter:    true,
	}
}

// retryDelay works out how long to wait before the next attempt. giveUp is set
// when the server's Retry-After is longer than the policy allows.
func retryDelay(p RetryPolicy, retryNumber int, resp *http.Response, now time.Time) (delay time.Duration, giveUp bool) {
	if p.RespectRetryAfter && resp != nil {
		if d := retryAfter(resp, now); d > 0 {
			if p.MaxBackoff > 0 && d > p.MaxBackoff {
				return 0, true
			}
			return d, false
		}
	}

	backoff := p.InitialBackoff
	if backoff <= 0 {
		return 0, false
	}
	for i := 1; i < retryNumber; i++ {
		if backoff > maxDuration/2 {
			backoff = maxDuration
			break
		}
		backoff *= 2
	}
	if p.MaxBackoff > 0 && backoff > p.MaxBackoff {
		backoff = p.MaxBackoff
	}
	if p.Jitter {
		return time.Duration(rand.Int63n(int64(backoff))), false
	}
	return backoff, false
}

// retryAfter parses the Retry-After header, either seconds or an HTTP date.
// Zero means missing, invalid or already in the past.
func retryAfter(resp *http.Response, now time.Time) time.Duration {
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil && secs >= 0 {
		if secs > int64(maxDuration/time.Second) {
			return maxDuration
		}
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(v); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}

// retryableNetErr says whether a transport error is worth another try.
// Cancellation and bad certificates won't fix themselves.
func retryableNetErr(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var (
		unknownAuth x509.UnknownAuthorityError
		certInvalid x509.CertificateInvalidError
		hostErr     x509.HostnameError
	)
	return !errors.As(err, &unknownAuth) && !errors.As(err, &certInvalid) && !errors.As(err, &hostErr)
}
