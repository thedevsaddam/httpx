# httpx 🚀

A small, fluent Go HTTP client built on `net/http` — with typed JSON decoding, tracing, streaming, retries, and configurable connection pooling.

[![Go](https://img.shields.io/badge/Go-1.18%2B-00ADD8?logo=go)](https://go.dev/)
![Dependencies](https://img.shields.io/badge/dependencies-standard%20library-brightgreen)

## ✨ Features

- **Fluent API** — `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, and `OPTIONS`.
- **Typed JSON** — decode responses with `ToJSON[T]`.
- **Connection pooling** — use defaults or configure pool limits.
- **Retry policy** — opt in to bounded retries with backoff.
- **Request/response hooks** — add metrics, logging, or instrumentation.
- **Response limits** — protect JSON and error-body helpers from unexpectedly large bodies.
- **Streaming** — copy large responses to a writer without buffering the whole body.
- **Typed HTTP errors** — inspect status and a bounded body excerpt with `errors.As`.
- **Auth and uploads** — built-in Bearer/Basic auth and multipart forms.
- **Diagnostics** — request logs and connection/timing details via `Debug()` and `Trace()`.
- **Tracing integration** — inject distributed trace headers through a custom `Tracer`.

## 📦 Installation

Use your module path when importing the package:

```bash
go get github.com/thedevsaddam/httpx
```

## 🚀 Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/thedevsaddam/httpx"
)

type Todo struct {
    ID        int    `json:"id"`
    Title     string `json:"title"`
    Completed bool   `json:"completed"`
}

func main() {
    client := httpx.New(
        httpx.WithBaseURL("https://jsonplaceholder.typicode.com"),
        httpx.WithTimeout(10*time.Second),
    )

    todo, err := httpx.ToJSON[Todo](
        client.GET("/todos/1").Send(context.Background()),
    )
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Todo #%d: %s\n", todo.ID, todo.Title)
}
```

## ⚙️ Configure the client

Set defaults once when you create the client. Existing options remain available alongside the newer options below.

```go
client := httpx.New(
    httpx.WithBaseURL("https://api.example.com/v1"),
    httpx.WithTimeout(10*time.Second),
    httpx.WithHeader("X-App-Name", "my-service"),
    httpx.WithAuth(httpx.NewBearerAuth("your-token")),
    httpx.WithMaxResponseBytes(5 << 20), // 5 MiB for helper reads
    httpx.WithPoolConfig(httpx.PoolConfig{
        MaxIdleConns:          100,
        MaxIdleConnsPerHost:   20,
        MaxConnsPerHost:       50,
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ResponseHeaderTimeout: 5 * time.Second,
    }),
)
```

### Available client options

| Option | What it does |
|---|---|
| `WithBaseURL(url)` | Resolves relative request paths against a base URL. |
| `WithTimeout(d)` | Sets the default request timeout (30 seconds by default). |
| `WithHeader(key, value)` | Adds a default header. |
| `WithQueryParam(key, value)` | Adds a default query parameter. |
| `WithAuth(auth)` | Sets default authentication. |
| `WithLogger(logger)` | Enables structured request logging. |
| `WithTracer(tracer)` | Injects tracing context into outgoing requests. |
| `WithTransport(transport)` | Uses a custom `*http.Transport`. |
| `WithHTTPClient(client)` | Uses a custom `*http.Client`. |
| `WithPoolConfig(cfg)` | Configures standard transport connection-pool limits and timeouts. |
| `WithMaxResponseBytes(n)` | Limits body reads performed by JSON/error helpers; does not limit manual streaming. |
| `WithRetryPolicy(policy)` | Enables retries according to a retry policy. |
| `WithRequestHook(hook)` | Runs a hook immediately before each network attempt. |
| `WithResponseHook(hook)` | Runs a hook after response headers arrive. |
| `WithClientName(name)` | Sets the `X-Client-Name` header. |

Pool settings with zero or negative values keep their transport defaults. Pool configuration applies when the client uses a standard `*http.Transport`.

## 📥 GET: query parameters and headers

```go
resp, err := client.GET("/users").
    QueryParam("status", "active").
    QueryParam("limit", "20").
    Header("Accept", "application/json").
    Send(ctx)
if err != nil {
    return err
}
defer resp.Close()

if !resp.IsSuccess() {
    return fmt.Errorf("API returned %s", resp.Status)
}
```

`IsSuccess()` returns `true` for status codes in the `2xx` range.

## 📤 POST JSON

```go
type CreateUser struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

user, err := httpx.ToJSON[User](
    client.POST("/users").
        JSON(CreateUser{Name: "Jane Doe", Email: "jane@example.com"}).
        Send(ctx),
)
if err != nil {
    return err
}
fmt.Println("Created user:", user.ID)
```

`JSON(v)` marshals `v` and sets `Content-Type: application/json`. Encoding errors are returned by `Send()`.

## 🚦 Handle HTTP errors

`ToJSON[T]` returns an `*HTTPError` for non-2xx responses. The error includes the status code, status text, URL, and a bounded body excerpt.

```go
user, err := httpx.ToJSON[User](client.GET("/users/123").Send(ctx))
if err != nil {
    var httpErr *httpx.HTTPError
    if errors.As(err, &httpErr) {
        log.Printf("HTTP %d (%s): %s", httpErr.StatusCode, httpErr.URL, httpErr.Body)
        return err
    }
    return err // network, timeout, or JSON decoding error
}
_ = user
```


## 🔁 Retry transient failures

Retries are **opt-in**. `MaxAttempts` includes the initial request. Retries are limited to supported idempotent methods and replayable request bodies; `POST` and other non-idempotent methods are not automatically retried.

```go
client := httpx.New(
    httpx.WithRetryPolicy(httpx.RetryPolicy{
        MaxAttempts:          3,
        InitialBackoff:       100 * time.Millisecond,
        MaxBackoff:           time.Second,
        RetryableStatusCodes: []int{429, 502, 503, 504},
        RetryNetworkErrors:   true,
    }),
)
```

Use a small attempt count and bounded backoff. Consider your API's rate limits and whether an operation is safe to repeat before enabling retries.

## 🪝 Request and response hooks

Hooks are useful for metrics, request correlation, and lightweight instrumentation.

```go
client := httpx.New(
    httpx.WithRequestHook(func(ctx context.Context, req *http.Request) {
        // Runs before each network attempt.
        // Example: add a correlation header.
        req.Header.Set("X-Request-Source", "billing-service")
    }),
    httpx.WithResponseHook(func(ctx context.Context, resp *http.Response) {
        // Runs after response headers arrive.
        // Example: record resp.StatusCode in your metrics system.
    }),
)
```

Add `"net/http"` to the imports for this example. Hooks can run concurrently; make shared state inside your hook thread-safe.

## 🌊 Stream large responses

`Send()` exposes the response body so you can process it as a stream. `CopyTo` copies the body to a writer and closes it when finished.

```go
resp, err := client.GET("/exports/archive").Send(ctx)
if err != nil {
    return err
}

file, err := os.Create("archive.zip")
if err != nil {
    _ = resp.Close()
    return err
}
defer file.Close()

if _, err := resp.CopyTo(file); err != nil {
    return err
}
```

Check `resp.IsSuccess()` or `resp.StatusCode` before saving the body when the endpoint can return an error payload.

`WithMaxResponseBytes` applies to `ToJSON` and error-body helper reads. It does not cap bytes copied manually through `CopyTo`, so streaming remains suitable for large downloads.

## 📎 Multipart and raw bodies

### Multipart upload

```go
resp, err := client.POST("/upload").
    Field("user_id", "12345").
    File("avatar", "./avatar.png").
    Send(ctx)
if err != nil {
    return err
}
defer resp.Close()

if !resp.IsSuccess() {
    return fmt.Errorf("upload failed: %s", resp.Status)
}
```

The current multipart implementation assembles the payload in memory. For very large uploads, account for that memory usage.

### Raw body

```go
resp, err := client.POST("/messages").
    Header("Content-Type", "text/plain").
    Body(strings.NewReader("hello" )).
    Send(ctx)
if err != nil {
    return err
}
defer resp.Close()
```

Add `"strings"` to the imports for this example.

## 🔐 Authentication

```go
// Bearer token
client := httpx.New(httpx.WithAuth(httpx.NewBearerAuth("your-token")))

// Or HTTP Basic authentication
client = httpx.New(httpx.WithAuth(httpx.NewBasicAuth("username", "password")))
```

You can also implement the `httpx.Auth` interface and apply it to a single request with `.Auth(auth)`.

## 🔍 Debugging and tracing

Enable diagnostics for one request:

```go
resp, err := client.GET("/health").Debug().Send(ctx)
if err != nil {
    return err
}
defer resp.Close()

fmt.Println(resp.Debug())
trace := resp.Trace()
fmt.Printf("status=%d duration=%s reused=%t\n",
    trace.StatusCode, trace.TotalTime, trace.IsConnReused)
```

`Trace()` returns a snapshot with fields such as DNS lookup time, TCP connection time, TLS handshake time, time to first byte, total time to response headers, connection reuse, and remote address. These are best-effort diagnostics; some values can be zero when connections are reused or a proxy is involved. `TotalTime` does not include consuming the response body.

Implement `httpx.Logger` to connect your structured logger, or `httpx.Tracer` to inject your tracing headers. The package does not create spans or define a propagation format for you.

## 🧵 Concurrency and response lifecycle

- A `Client` is intended to be reused for concurrent requests.
- Do not mutate a request builder while another goroutine is using it; prefer one builder per request.
- Hooks and custom `Auth`, `Logger`, and `Tracer` implementations should be safe for concurrent calls.
- Close response bodies when you consume them directly. `ToJSON[T]` closes the body automatically.
- `Response.Close()` drains up to 4 KiB before closing to help connection reuse; fully consuming the body is best when practical.

## 🧭 API cheat sheet

| Call | Purpose |
|---|---|
| `httpx.New(options...)` | Create a client. |
| `client.GET(url)` / `POST` / `PUT` / `PATCH` / `DELETE` | Start a request. `HEAD` and `OPTIONS` are also supported. |
| `.Header(k, v)` / `.Headers(map)` | Set request headers. |
| `.QueryParam(k, v)` / `.QueryParams(map)` | Add query parameters. |
| `.JSON(value)` / `.Body(reader)` | Set request body. |
| `.Field(k, v)` / `.File(field, path)` | Add multipart form values/files. |
| `.Auth(auth)` / `.Timeout(d)` | Override request authentication or timeout. |
| `.Debug()` / `resp.Trace()` / `resp.Debug()` | Inspect request diagnostics. |
| `.Send(ctx)` | Execute the request. |
| `resp.IsSuccess()` / `resp.Close()` / `resp.CopyTo(w)` | Check, close, or stream a response. |
| `httpx.ToJSON[T](resp, err)` | Decode JSON and close the response body. |

## 🧪 Tests

Run the package tests and race detector from the module root:

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Requirements

- Go 1.22 or newer for the module configuration shown here (the package uses Go generics, which require Go 1.18+).
- Avoid logging credentials, authorization headers, or sensitive query parameters in production.
