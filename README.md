# httpx 🚀

A small, fluent Go HTTP client built on `net/http` with typed JSON decoding, tracing, streaming, retries, and configurable connection pooling.

[![Go](https://img.shields.io/badge/Go-1.18%2B-00ADD8?logo=go)](https://go.dev/)
![Dependencies](https://img.shields.io/badge/dependencies-standard%20library-brightgreen)

## Features

- Fluent builders for `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`, and `QUERY`
- Generic JSON decoding with `ToJSON[T]`
- Configurable timeout and connection pooling
- Optional retries with bounded backoff
- Request and response hooks for instrumentation
- Bounded response reads and typed `HTTPError` values
- Streaming response copies with `Response.CopyTo`
- Bearer and Basic authentication, plus custom auth providers
- Multipart uploads, custom headers, query parameters, and raw bodies
- Structured logging and `httptrace`-based request diagnostics

## Requirements

- Go 1.22 or newer

## Installation

```bash
go get github.com/thedevsaddam/httpx
```


## Quick start

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
        client.GET("/todos/1").Do(context.Background()),
    )
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Todo #%d: %s\n", todo.ID, todo.Title)
}
```

`ToJSON[T]` decodes a successful JSON response and closes the response body automatically.

## Common request examples

### GET with query parameters

```go
resp, err := client.GET("/users").
    QueryParam("status", "active").
    QueryParam("limit", "20").
    Header("Accept", "application/json").
    Do(ctx)
if err != nil {
    return err
}
defer resp.Close()

if !resp.IsSuccess() {
    return fmt.Errorf("request failed: %s", resp.Status)
}
```

### POST JSON

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
        Do(ctx),
)
if err != nil {
    return err
}
fmt.Println("Created user:", user.ID)
```

### POST URL-encoded form
Use `Form` to send `application/x-www-form-urlencoded` data, for example when requesting an OAuth token. This is different from Field, which adds a field to a multipart request.

```go
resp, err := client.POST("/oauth/token").
    // DO NOT use both Form and FormValues in a single request
    // For simple key value style
    Form(map[string]string{
        "grant_type":    "client_credentials",
        "client_id":     "my-client",
        "client_secret": "my-secret",
    }).
    // For repeated values per key.
    FormValues(url.Values{
        "grant_type":    "client_credentials",
        "client_id":     "my-client",
        "client_secret": "my-secret",
        "tag":    {"golang", "backend"},
    }).
    Do(ctx)
if err != nil {
    return err
}
defer resp.Close()

if !resp.IsSuccess() {
    return fmt.Errorf("token request failed: %s", resp.Status)
}
```

`Form` URL-encodes the values and sets `Content-Type: application/x-www-form-urlencoded`.


## Streaming responses

`Send` returns a response whose body can be read directly. Use `CopyTo` to stream a large response to a writer without buffering the entire body in memory:

```go
resp, err := client.GET("/exports/archive").Do(ctx)
if err != nil {
    return err
}
if !resp.IsSuccess() {
    defer resp.Close()
    return fmt.Errorf("download failed: %s", resp.Status)
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

`WithMaxResponseBytes` applies to `ToJSON` and error-body helper reads; it does not limit manual streaming.

## Multipart uploads and raw bodies

### Multipart upload

```go
resp, err := client.POST("/upload").
    Field("user_id", "12345").
    Fields(url.Values{"tag": {"tag-1", "tag-2"}}. // with repetitive values
    File("avatar", "./avatar.png"). // from file path
    FileReader("attachments", bytes.NewReader(report), "report.pdf"). // from memory
    FileReader("attachments", logFile, "app.log"). // from any reader
    FileReader("attachments", pdfReader, "report.pdf", "application/pdf"). // from reader with content-type
    Do(ctx)
if err != nil {
    return err
}
defer resp.Close()

if !resp.IsSuccess() {
    return fmt.Errorf("upload failed: %s", resp.Status)
}
```

The current multipart implementation assembles the upload payload in memory. Consider this when uploading large files.

### Raw request body

```go
resp, err := client.POST("/messages").
    Header("Content-Type", "text/plain").
    Body(strings.NewReader("hello" )).
    Do(ctx)
if err != nil {
    return err
}
defer resp.Close()
```


### Handle HTTP errors

When `ToJSON[T]` receives a non-2xx response, it returns an `*httpx.HTTPError`. Use `errors.As` to inspect it:

```go
user, err := httpx.ToJSON[User](client.GET("/users/123").Do(ctx))
if err != nil {
    var httpErr *httpx.HTTPError
    if errors.As(err, &httpErr) {
        log.Printf("HTTP %d for %s: %s", httpErr.StatusCode, httpErr.URL, httpErr.Body)
    }
    return err
}
_ = user
```

`HTTPError.Body` is a bounded excerpt, not necessarily the full response. Avoid logging it if it could contain secrets or personal information.

## Client configuration

Configure the client once and reuse it across requests:

```go
client := httpx.New(
    httpx.WithBaseURL("https://api.example.com/v1"),
    httpx.WithClientName("billing-service"),
    httpx.WithTimeout(10*time.Second),
    httpx.WithHeader("Content-Type", "application/json"),
    httpx.WithHeader("Accept", "application/json"),
    httpx.WithAuth(httpx.NewBearerAuth("<YOUR_TOKEN>")),
    httpx.WithMaxResponseBytes(5 << 20), // 5 MiB for helper reads
    httpx.WithRetryPolicy(httpx.StandardRetryPolicy()),
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

### Configuration options

| Option | Description |
| --- | --- |
| `WithBaseURL(url)` | Base URL for relative request paths. |
| `WithTimeout(d)` | Default request timeout; 30 seconds by default. |
| `WithHeader(key, value)` | Default request header. |
| `WithQueryParam(key, value)` | Default query parameter. |
| `WithClientName(name)` | Sets the `X-Client-Name` header. |
| `WithAuth(auth)` | Default authentication provider. |
| `WithLogger(logger)` | Structured request logging. |
| `WithTracer(tracer)` | Injects distributed-tracing context into requests. |
| `WithPoolConfig(cfg)` | Configures the standard transport's connection pool and timeouts. |
| `WithMaxResponseBytes(n)` | Maximum body size read by JSON and HTTP-error helpers; default is 10 MiB. |
| `WithRetryPolicy(policy)` | Enables bounded retries. Retries are disabled by default. |
| `WithRequestHook(hook)` | Hook called before each network attempt. |
| `WithResponseHook(hook)` | Hook called after response headers arrive. |
| `WithTransport(transport)` | Supplies a custom `*http.Transport`. |
| `WithHTTPClient(client)` | Supplies a custom `*http.Client`. |

For `PoolConfig`, zero values keep the transport defaults. Pool settings apply when the client uses a standard `*http.Transport`.

## Retries

`MaxAttempts` includes the initial request. For a standard policy, use `StandardRetryPolicy()`: three total attempts, retries for network errors and HTTP 429/502/503/504, capped exponential backoff with full jitter, and support for the `Retry-After` header (capped by `MaxBackoff`).

```go
client := httpx.New(
    httpx.WithRetryPolicy(httpx.StandardRetryPolicy()),
)
```

For a custom policy, set `Jitter: true` to randomize each delay between zero and its exponential backoff cap, and `RespectRetryAfter: true` to honor `Retry-After` on retryable responses:

```go
client := httpx.New(
    httpx.WithRetryPolicy(httpx.RetryPolicy{
        MaxAttempts:          3,
        InitialBackoff:       100 * time.Millisecond,
        MaxBackoff:           time.Second,
        RetryableStatusCodes: []int{429, 502, 503, 504},
        RetryNetworkErrors:   true,
        Jitter:               true,
        RespectRetryAfter:    true,
    }),
)
```

By default, retries are restricted to supported idempotent methods (`GET`, `HEAD`, `OPTIONS`, `DELETE`, and `QUERY`) and replayable request bodies. To explicitly opt a request such as `POST` into the configured retry policy, call `.Retryable()`. The body must be replayable (for example, one set with `.JSON(...)`), and repeating the operation must be safe for your application. 
For operations that create payments, orders, or other side effects, use a server-supported idempotency key; the client cannot guarantee deduplication by itself.

```go
resp, err := client.POST("/orders").
    Header("Idempotency-Key", "order-12345").
    JSON(payload).
    Retryable().
    Do(ctx)
```

## Hooks

Use hooks for metrics, correlation IDs, or instrumentation:

```go
client := httpx.New(
    httpx.WithRequestHook(func(ctx context.Context, req *http.Request) {
        req.Header.Set("X-Request-Source", "billing-service")
    }),
    httpx.WithResponseHook(func(ctx context.Context, resp *http.Response) {
        // Record status and latency in your metrics system.
    }),
)
```

Hooks may run concurrently, so protect any shared state they access.

## Authentication

```go
// Bearer token
client := httpx.New(httpx.WithAuth(httpx.NewBearerAuth("your-token")))

// Basic authentication
client = httpx.New(httpx.WithAuth(httpx.NewBasicAuth("username", "password")))

// API key authentication
client = httpx.New(httpx.WithAuth(httpx.NewAPIKeyAuth("X-API-Key", "your-api-key")))
```

You can implement the `httpx.Auth` interface to provide custom authentication. A request can override the client's default with `.Auth(auth)`.

## Logging and tracing

Implement `httpx.Logger` to connect your preferred structured logger:

```go
type MyLogger struct{}

func (l *MyLogger) Debug(ctx context.Context, msg string, kv ...any) {}
func (l *MyLogger) Info(ctx context.Context, msg string, kv ...any)  {}
func (l *MyLogger) Error(ctx context.Context, msg string, kv ...any) {}
```

Implement `httpx.Tracer` to inject tracing headers:

```go
type MyTracer struct{}

func (t *MyTracer) Inject(ctx context.Context, req *http.Request) {
    // Inject trace context into req.
}
```

Call `.Debug()` on an individual request to log timing and connection diagnostics. Read the trace snapshot with `resp.Trace()` or the formatted report with `resp.Debug()`. Trace timings are best-effort and `TotalTime` ends when response headers arrive, not when the body has been fully read.

## Concurrency and response lifecycle

- Reuse a configured client for concurrent requests; the underlying `net/http` client and transport support concurrent use.
- Build a separate request builder for each concurrent operation. Do not mutate and send the same builder concurrently.
- Hooks and custom auth, logger, and tracer implementations must be safe for concurrent use if shared by requests.
- Always close response bodies when reading responses directly. `ToJSON[T]` closes the body automatically.
- `Response.Close()` closes the body and attempts to drain a small amount to improve connection reuse.

## Testing

Run the test suite and static checks from the module root:

```bash
go test ./...
go test -race ./...
go vet ./...
```

## API reference

| API | Description |
| --- | --- |
| `New(options...)` | Creates a client with pooling enabled by default. |
| `client.GET(url)` / `POST` / `PUT` / `PATCH` / `DELETE` / `HEAD` / `OPTIONS` / `QUERY` | Creates a request builder. |
| `builder.Header(key, value)` / `Headers(map)` | Sets request headers. |
| `builder.QueryParam(key, value)` / `QueryParams(map)` | Adds query parameters. |
| `builder.JSON(value)` | Encodes a JSON request body. |
| `builder.Body(reader)` | Sets a raw request body. |
| `builder.File(field, path)` / `Field(key, value)` | Adds multipart fields. |
| `builder.Auth(auth)` / `Timeout(d)` | Overrides request authentication or timeout. |
| `builder.Debug()` | Enables request diagnostics. |
| `builder.Do(ctx)` | Executes the request. |
| `resp.IsSuccess()` | Reports whether the status code is 2xx. |
| `resp.CopyTo(writer)` | Streams the response body to a writer and closes it. |
| `resp.Close()` | Closes the response body. |
| `resp.Trace()` / `resp.Debug()` | Returns trace information or a formatted report. |
| `ToJSON[T](resp, err)` | Decodes JSON and closes the response body. |

## License
The **httpx** is an open-source software licensed under the [MIT License](LICENSE.md).
