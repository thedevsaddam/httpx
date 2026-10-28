package httpx

import (
	"errors"
	"net/http"
)

var errNilRequest = errors.New("httpx: nil request")

// BearerAuth sends an Authorization: Bearer header.
type BearerAuth struct{ Token string }

func (b BearerAuth) Apply(req *http.Request) error {
	if req == nil {
		return errNilRequest
	}
	req.Header.Set("Authorization", "Bearer "+b.Token)
	return nil
}

func NewBearerAuth(token string) Auth { return BearerAuth{Token: token} }

// BasicAuth sends HTTP Basic credentials.
type BasicAuth struct{ Username, Password string }

func (b BasicAuth) Apply(req *http.Request) error {
	if req == nil {
		return errNilRequest
	}
	req.SetBasicAuth(b.Username, b.Password)
	return nil
}

func NewBasicAuth(username, password string) Auth {
	return BasicAuth{Username: username, Password: password}
}

// APIKeyAuth sends an API key in a header such as X-API-Key.
type APIKeyAuth struct {
	Header string
	Key    string
}

func (a APIKeyAuth) Apply(req *http.Request) error {
	if req == nil {
		return errNilRequest
	}
	if a.Header == "" || a.Key == "" {
		return errors.New("httpx: API key auth needs a header name and a key")
	}
	req.Header.Set(a.Header, a.Key)
	return nil
}

func NewAPIKeyAuth(header, key string) Auth {
	return APIKeyAuth{Header: header, Key: key}
}

// APIKeyQueryAuth sends an API key as a query parameter. Prefer the header
// variant when the server supports it: httpx keeps query strings out of its
// logs and errors, but proxies and servers may still record them.
type APIKeyQueryAuth struct {
	Param string
	Key   string
}

func (a APIKeyQueryAuth) Apply(req *http.Request) error {
	if req == nil {
		return errNilRequest
	}
	if a.Param == "" || a.Key == "" {
		return errors.New("httpx: API key query auth needs a parameter name and a key")
	}
	q := req.URL.Query()
	q.Set(a.Param, a.Key)
	req.URL.RawQuery = q.Encode()
	return nil
}

func NewAPIKeyQueryAuth(param, key string) Auth {
	return APIKeyQueryAuth{Param: param, Key: key}
}
