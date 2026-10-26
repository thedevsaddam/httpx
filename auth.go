package httpx

import (
	"errors"
	"net/http"
)

// BearerAuth applies a bearer token.
type BearerAuth struct{ Token string }

func (b BearerAuth) Apply(req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+b.Token)
	return nil
}
func NewBearerAuth(token string) Auth { return BearerAuth{Token: token} }

// BasicAuth applies HTTP Basic authentication.
type BasicAuth struct{ Username, Password string }

func (b BasicAuth) Apply(req *http.Request) error {
	if req == nil {
		return errors.New("httpx: cannot apply basic auth to a nil request")
	}
	req.SetBasicAuth(b.Username, b.Password)
	return nil
}

// NewBasicAuth creates an HTTP Basic authentication provider.
func NewBasicAuth(username, password string) Auth {
	return BasicAuth{Username: username, Password: password}
}

// APIKeyAuth sends an API key in a request header, for example X-API-Key.
// Header must be a non-empty header name and Key must be non-empty.
type APIKeyAuth struct {
	Header string
	Key    string
}

func (a APIKeyAuth) Apply(req *http.Request) error {
	if req == nil {
		return errors.New("httpx: cannot apply API key auth to a nil request")
	}
	if a.Header == "" {
		return errors.New("httpx: API key header name must not be empty")
	}
	if a.Key == "" {
		return errors.New("httpx: API key must not be empty")
	}
	req.Header.Set(a.Header, a.Key)
	return nil
}

// NewAPIKeyAuth creates an API key authentication provider that sends key in header.
// Example: NewAPIKeyAuth("X-API-Key", "your-api-key").
func NewAPIKeyAuth(header, key string) Auth {
	return APIKeyAuth{Header: header, Key: key}
}

// APIKeyQueryAuth sends an API key as a URL query parameter. Prefer header-based
// API keys when the server supports them. httpx redacts query strings from its logs,
// traces and errors, but proxies and servers may still record them.
type APIKeyQueryAuth struct {
	Param string
	Key   string
}

func (a APIKeyQueryAuth) Apply(req *http.Request) error {
	if req == nil {
		return errors.New("httpx: cannot apply API key query auth to a nil request")
	}
	if a.Param == "" {
		return errors.New("httpx: API key query parameter must not be empty")
	}
	if a.Key == "" {
		return errors.New("httpx: API key must not be empty")
	}
	q := req.URL.Query()
	q.Set(a.Param, a.Key)
	req.URL.RawQuery = q.Encode()
	return nil
}

// NewAPIKeyQueryAuth creates an API key authentication provider that sends key
// in a query parameter. Example: NewAPIKeyQueryAuth("api_key", "your-api-key").
func NewAPIKeyQueryAuth(param, key string) Auth {
	return APIKeyQueryAuth{Param: param, Key: key}
}
