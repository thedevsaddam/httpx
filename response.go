package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
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

// isJSONContentType reports whether the Content-Type is application/json or
// a +json type such as application/problem+json. A missing header counts as
// JSON, since plenty of APIs leave it out; text/html, text/plain and the like
// do not.
func isJSONContentType(h http.Header) bool {
	v := h.Get("Content-Type")
	if strings.TrimSpace(v) == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(v)
	if err != nil {
		return false
	}
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
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
		// only JSON bodies are decoded, so anything else stays a short excerpt
		decode := resp.errTarget != nil && isJSONContentType(resp.Header)
		readLimit := int64(errorBodyExcerpt + 1)
		if decode {
			readLimit = resp.maxBodyBytes
			if readLimit <= 0 {
				readLimit = defaultMaxResponse
			}
			readLimit++
		}
		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, readLimit))
		if readErr != nil && len(excerpt) == 0 {
			return nil, readErr
		}
		httpErr := &HTTPError{
			Method: resp.method, URL: resp.safeURL, StatusCode: resp.StatusCode,
			Status: resp.Status, Header: resp.Header.Clone(), Body: string(excerpt),
		}
		if decode && readErr == nil && int64(len(excerpt)) < readLimit &&
			len(bytes.TrimSpace(excerpt)) > 0 && json.Unmarshal(excerpt, resp.errTarget) == nil {
			httpErr.Decoded = true
		}
		if len(excerpt) > errorBodyExcerpt {
			httpErr.Body = string(excerpt[:errorBodyExcerpt]) + "…[truncated]"
		}
		return nil, httpErr
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
