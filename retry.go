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
