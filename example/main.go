package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/thedevsaddam/httpx"
)

// A practical, runnable tour of the httpx package.
// Run from a project where github.com/thedevsaddam/httpx is your module path:
//   go run main.go
//
// These examples use https://httpbin.org, a public HTTP testing service.
// Network access is required. Each example reports its own errors so one
// failed demo does not prevent the remaining examples from running.

type Todo struct {
	UserID    int    `json:"userId"`
	ID        int    `json:"id"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
}

type HTTPBinResponse struct {
	Args    map[string]any    `json:"args"`
	Data    string            `json:"data"`
	Files   map[string]string `json:"files"`
	Form    map[string]string `json:"form"`
	Headers map[string]string `json:"headers"`
	JSON    map[string]any    `json:"json"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Configure a reusable client once. Requests can share this client safely.
	client := httpx.New(
		httpx.WithBaseURL("https://httpbin.org"),
		httpx.WithTimeout(12*time.Second),
		httpx.WithHeader("Accept", "application/json"),
		httpx.WithClientName("httpx-example"),
		httpx.WithPoolConfig(httpx.PoolConfig{
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       20,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 8 * time.Second,
		}),
		httpx.WithMaxResponseBytes(2<<20), // 2 MiB for helper reads such as ToJSON.
		httpx.WithRetryPolicy(httpx.RetryPolicy{
			MaxAttempts:          3, // Includes the first attempt.
			InitialBackoff:       150 * time.Millisecond,
			MaxBackoff:           time.Second,
			RetryableStatusCodes: []int{http.StatusTooManyRequests, 502, 503, 504},
			RetryNetworkErrors:   true,
		}),
		httpx.WithRequestHook(func(ctx context.Context, req *http.Request) {
			// Hooks run for each network attempt. Keep them quick and concurrency-safe.
			log.Printf("--> %s %s", req.Method, req.URL.Redacted())
		}),
		httpx.WithResponseHook(func(ctx context.Context, resp *http.Response) {
			log.Printf("<-- %s %s", resp.Status, resp.Request.URL.Redacted())
		}),
	)

	runExample("GET with query parameters", func() { getExample(ctx, client) })
	runExample("POST JSON", func() { postJSONExample(ctx, client) })
	runExample("request-specific headers and timeout", func() { headersExample(ctx, client) })
	runExample("PUT, PATCH, and DELETE", func() { methodsExample(ctx, client) })
	runExample("typed HTTP errors", func() { httpErrorExample(ctx, client) })
	runExample("manual response handling", func() { manualResponseExample(ctx, client) })
	runExample("stream response to a file", func() { streamingExample(ctx, client) })
	runExample("request trace debugging", func() { debugExample(ctx, client) })
	runExample("multipart upload", func() { uploadExample(ctx, client) })
	runExample("raw text body", func() { rawBodyExample(ctx, client) })
}

func runExample(name string, fn func()) {
	fmt.Printf("\n=== %s ===\n", name)
	fn()
}

func getExample(ctx context.Context, client *httpx.Client) {
	result, err := httpx.ToJSON[HTTPBinResponse](
		client.GET("/get").
			QueryParam("name", "Jane Doe").
			QueryParam("page", "2").
			QueryParam("tag", "go").
			QueryParam("tag", "httpx").
			Send(ctx),
	)
	if err != nil {
		log.Printf("GET failed: %v", err)
		return
	}
	fmt.Printf("URL:  %s\nArgs: %#v\n", result.URL, result.Args)

	// For a map of single values, use .QueryParams(map[string]string{...}).
}

func postJSONExample(ctx context.Context, client *httpx.Client) {
	payload := map[string]any{
		"name":  "Jane Doe",
		"email": "jane@example.com",
		"roles": []string{"developer", "admin"},
	}
	result, err := httpx.ToJSON[HTTPBinResponse](
		client.POST("/anything/users").JSON(payload).Send(ctx),
	)
	if err != nil {
		log.Printf("POST failed: %v", err)
		return
	}
	fmt.Printf("Method: %s\nURL: %s\nJSON: %#v\n", result.Method, result.URL, result.JSON)
}

func headersExample(ctx context.Context, client *httpx.Client) {
	result, err := httpx.ToJSON[HTTPBinResponse](
		client.GET("/anything/headers").
			Header("X-Request-ID", "demo-123").
			Header("X-Environment", "local").
			Timeout(4 * time.Second).
			Send(ctx),
	)
	if err != nil {
		log.Printf("Headers example failed: %v", err)
		return
	}
	fmt.Printf("Returned headers: %#v\n", result.Headers)
}

func methodsExample(ctx context.Context, client *httpx.Client) {
	payload := map[string]string{"name": "Updated item"}
	requests := []struct {
		method string
		send   func() (*httpx.Response, error)
	}{
		{"PUT", func() (*httpx.Response, error) { return client.PUT("/anything/items/42").JSON(payload).Send(ctx) }},
		{"PATCH", func() (*httpx.Response, error) { return client.PATCH("/anything/items/42").JSON(payload).Send(ctx) }},
		{"DELETE", func() (*httpx.Response, error) { return client.DELETE("/anything/items/42").Send(ctx) }},
	}
	for _, item := range requests {
		resp, err := item.send()
		if err != nil {
			log.Printf("%s failed: %v", item.method, err)
			continue
		}
		fmt.Printf("%s -> %s\n", item.method, resp.Status)
		_ = resp.Close()
	}
}

func httpErrorExample(ctx context.Context, client *httpx.Client) {
	// /status/404 intentionally returns a non-2xx response.
	_, err := httpx.ToJSON[HTTPBinResponse](client.GET("/status/404").Send(ctx))
	if err == nil {
		log.Printf("expected a 404 error, got nil")
		return
	}
	var httpErr *httpx.HTTPError
	if errors.As(err, &httpErr) {
		fmt.Printf("Status: %d (%s)\nURL: %s\nBody excerpt: %s\n",
			httpErr.StatusCode, httpErr.Status, httpErr.URL, httpErr.Body)
		return
	}
	log.Printf("request failed for another reason: %v", err)
}

func manualResponseExample(ctx context.Context, client *httpx.Client) {
	resp, err := client.GET("/status/200").Send(ctx)
	if err != nil {
		log.Printf("Manual response failed: %v", err)
		return
	}
	defer resp.Close() // Always close manually handled response bodies.

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Read body failed: %v", err)
		return
	}
	fmt.Printf("Status: %s | success: %t | body: %s\n", resp.Status, resp.IsSuccess(), body)
}

func streamingExample(ctx context.Context, client *httpx.Client) {
	// CopyTo streams the body instead of buffering the whole response in memory.
	// It closes the response body when the copy finishes.
	file, err := os.CreateTemp("", "httpx-download-*.bin")
	if err != nil {
		log.Printf("Create output file failed: %v", err)
		return
	}
	path := file.Name()
	defer os.Remove(path)
	defer file.Close()

	resp, err := client.GET("/bytes/65536").Send(ctx) // 64 KiB test payload.
	if err != nil {
		log.Printf("Download request failed: %v", err)
		return
	}
	if !resp.IsSuccess() {
		fmt.Printf("Download returned %s\n", resp.Status)
		_ = resp.Close()
		return
	}
	n, err := resp.CopyTo(file)
	if err != nil {
		log.Printf("Stream copy failed: %v", err)
		return
	}
	fmt.Printf("Streamed %d bytes to %s\n", n, path)
}

func debugExample(ctx context.Context, client *httpx.Client) {
	resp, err := client.GET("/get").QueryParam("debug", "true").Debug().Send(ctx)
	if err != nil {
		log.Printf("Debug request failed: %v", err)
		return
	}
	defer resp.Close()

	fmt.Println(resp.Debug())
	trace := resp.Trace()
	fmt.Printf("Method=%s status=%d total=%s reused_connection=%t\n",
		trace.Method, trace.StatusCode, trace.TotalTime, trace.IsConnReused)
}

func uploadExample(ctx context.Context, client *httpx.Client) {
	file, err := os.CreateTemp("", "httpx-upload-*.txt")
	if err != nil {
		log.Printf("Create upload file failed: %v", err)
		return
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString("Hello from httpx multipart upload!\n"); err != nil {
		_ = file.Close()
		log.Printf("Write upload file failed: %v", err)
		return
	}
	if err := file.Close(); err != nil {
		log.Printf("Close upload file failed: %v", err)
		return
	}

	result, err := httpx.ToJSON[HTTPBinResponse](
		client.POST("/post").
			Field("description", "example upload").
			File("file", path).
			Send(ctx),
	)
	if err != nil {
		log.Printf("Upload failed: %v", err)
		return
	}
	fmt.Printf("Upload accepted: method=%s form=%#v files=%#v\n", result.Method, result.Form, result.Files)
}

func rawBodyExample(ctx context.Context, client *httpx.Client) {
	result, err := httpx.ToJSON[HTTPBinResponse](
		client.POST("/anything/raw").
			Header("Content-Type", "text/plain; charset=utf-8").
			Body(strings.NewReader("plain text payload from httpx")).
			Send(ctx),
	)
	if err != nil {
		log.Printf("Raw body request failed: %v", err)
		return
	}
	fmt.Printf("Method: %s\nData: %q\n", result.Method, result.Data)
}
