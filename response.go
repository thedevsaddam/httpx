package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
)

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
	if r == nil || r.Response == nil || r.Body == nil {
		return nil
	}
	// A 101 body is the live upgraded connection; draining it would block until the peer writes.
	if r.StatusCode != http.StatusSwitchingProtocols {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, drainLimit))
	}
	return r.Body.Close()
}

// CopyTo streams the response body to dst without buffering the entire response in memory.
// It closes the response body when finished.
func (r *Response) CopyTo(dst io.Writer) (int64, error) {
	if r == nil || r.Response == nil || r.Body == nil {
		return 0, errors.New("httpx: nil response")
	}
	defer r.Close()
	return io.Copy(dst, r.Body)
}

// IsSuccess reports whether the response status is 2xx.
func (r *Response) IsSuccess() bool {
	return r != nil && r.Response != nil && r.StatusCode >= 200 && r.StatusCode < 300
}

// readAll reads and closes the body of a successful response, enforcing the size limit.
// Non-2xx responses become *HTTPError.
func readAll(resp *Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Response == nil || resp.Body == nil {
		return nil, errors.New("httpx: nil response")
	}
	defer resp.Close()

	if !resp.IsSuccess() {
		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, errorBodyExcerpt+1))
		body := string(excerpt)
		if len(excerpt) > errorBodyExcerpt {
			body = string(excerpt[:errorBodyExcerpt]) + "…[truncated]"
		}
		if readErr != nil && len(excerpt) == 0 {
			return nil, readErr
		}
		return nil, &HTTPError{
			Method: resp.method, URL: resp.safeURL, StatusCode: resp.StatusCode,
			Status: resp.Status, Header: resp.Header.Clone(), Body: body,
		}
	}

	limit := resp.maxBodyBytes
	if limit <= 0 {
		limit = defaultMaxResponse
	}
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("httpx: response body exceeds %d bytes", limit)
	}
	if readErr != nil {
		return nil, fmt.Errorf("httpx: failed to read response body: %w", readErr)
	}
	return data, nil
}

// ToJSON reads, closes, and decodes a successful response into T.
// An empty body (for example 204) returns ErrEmptyBody.
func ToJSON[T any](resp *Response, err error) (T, error) {
	var zero T
	data, err := readAll(resp, err)
	if err != nil {
		return zero, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return zero, ErrEmptyBody
	}
	var target T
	if err := json.Unmarshal(data, &target); err != nil {
		return zero, fmt.Errorf("httpx: failed to decode response JSON: %w", err)
	}
	return target, nil
}

// ToBytes reads and closes a successful response body, subject to the max response size.
func ToBytes(resp *Response, err error) ([]byte, error) { return readAll(resp, err) }

// ToString reads and closes a successful response body as a string.
func ToString(resp *Response, err error) (string, error) {
	data, err := readAll(resp, err)
	return string(data), err
}

// IsTimeout reports whether err is a timeout or deadline error.
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
