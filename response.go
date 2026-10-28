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

// cancelOnCloseBody keeps the timeout context alive until the caller is done
// with the body, then releases it.
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

// Close drains up to 4 KiB so the connection can be reused, then closes the body.
// The cap stops a huge response from blocking Close.
func (r *Response) Close() error {
	if r == nil || r.Response == nil || r.Body == nil {
		return nil
	}
	// on a 101 the body is the live connection, draining it would hang
	if r.StatusCode != http.StatusSwitchingProtocols {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, drainLimit))
	}
	return r.Body.Close()
}

// CopyTo streams the body into dst and closes it afterwards.
func (r *Response) CopyTo(dst io.Writer) (int64, error) {
	if r == nil || r.Response == nil || r.Body == nil {
		return 0, errors.New("httpx: nil response")
	}
	defer r.Close()
	return io.Copy(dst, r.Body)
}

// IsSuccess reports whether the status is 2xx.
func (r *Response) IsSuccess() bool {
	return r != nil && r.Response != nil && r.StatusCode >= 200 && r.StatusCode < 300
}

// readAll reads and closes the body of a 2xx response within the size limit.
// Any other status comes back as *HTTPError.
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

// ToJSON decodes a 2xx response into T and closes the body.
// An empty body returns ErrEmptyBody.
func ToJSON[T any](resp *Response, err error) (T, error) {
	var zero T
	data, err := readAll(resp, err)
	if err != nil {
		return zero, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return zero, ErrEmptyBody
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return zero, fmt.Errorf("httpx: failed to decode response JSON: %w", err)
	}
	return out, nil
}

// ToBytes reads and closes the body of a 2xx response.
func ToBytes(resp *Response, err error) ([]byte, error) { return readAll(resp, err) }

// ToString is like ToBytes but returns a string.
func ToString(resp *Response, err error) (string, error) {
	data, err := readAll(resp, err)
	return string(data), err
}

// IsTimeout reports whether err is a timeout or a deadline error.
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
