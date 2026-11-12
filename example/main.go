// Command example is a runnable demo of the httpx HTTP client.
//
// It walks through a full CRUD cycle against the public JSONPlaceholder
// API (https://jsonplaceholder.typicode.com), so no setup, API keys, or
// local server are needed.
//
// What it demonstrates:
//   - Building a client with a base URL, timeout, and default headers
//   - GET (list with query params, and single item with a path param)
//   - POST with a JSON body
//   - PUT (full replace) and PATCH (partial update using pointer fields)
//   - DELETE with a status check
//   - Decoding responses into typed structs with httpx.ToJSON
//
// Run it:
//
//	go run main.go
//
// Note: JSONPlaceholder is a fake API. Writes (POST/PUT/PATCH/DELETE) return
// realistic responses, but nothing is actually persisted.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thedevsaddam/httpx"
)

const (
	baseURL     = "https://jsonplaceholder.typicode.com"
	listLimit   = 5
	exampleID   = 1
	exampleUser = 1
)

// Todo represents a JSONPlaceholder todo resource.
type Todo struct {
	ID        int    `json:"id"`
	UserID    int    `json:"userId"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
}

// CreateTodo is the payload for creating a todo.
type CreateTodo struct {
	UserID    int    `json:"userId"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
}

// UpdateTodo is the payload for replacing a todo (PUT).
// Every field is sent, so a PUT overwrites the whole resource.
type UpdateTodo struct {
	ID        int    `json:"id"`
	UserID    int    `json:"userId"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
}

// PatchTodo is the payload for a partial update (PATCH).
// Pointer fields let us tell an omitted field apart from its zero value,
// e.g. Completed=false must be sent, while Completed=nil must not.
type PatchTodo struct {
	Title     *string `json:"title,omitempty"`
	Completed *bool   `json:"completed,omitempty"`
}

// Client is a small typed wrapper around httpx for the todos API.
type Client struct {
	*httpx.Client
}

// NewClient builds a client with a base URL, timeout and default headers.
func NewClient() *Client {
	return &Client{
		httpx.New(
			httpx.WithBaseURL(baseURL),
			httpx.WithTimeout(10*time.Second),
			httpx.WithHeader("Accept", "application/json"),
		),
	}
}

// ListTodos calls GET /todos, optionally filtered with ?userId=<userID>.
// Pass 0 to skip the filter. Results are capped by listLimit.
func (c *Client) ListTodos(ctx context.Context, userID int) ([]Todo, error) {
	req := c.GET("/todos").QueryParam("_limit", fmt.Sprint(listLimit))

	if userID > 0 {
		req = req.QueryParam("userId", fmt.Sprint(userID))
	}

	return httpx.ToJSON[[]Todo](req.Do(ctx))
}

// GetTodo calls GET /todos/{id}.
func (c *Client) GetTodo(ctx context.Context, id int) (Todo, error) {
	return httpx.ToJSON[Todo](
		c.GET("/todos/{id}").PathParam("id", fmt.Sprint(id)).Do(ctx),
	)
}

// CreateTodo calls POST /todos.
func (c *Client) CreateTodo(ctx context.Context, input CreateTodo) (Todo, error) {
	return httpx.ToJSON[Todo](
		c.POST("/todos").JSON(input).Do(ctx),
	)
}

// UpdateTodo calls PUT /todos/{id} (full replacement).
func (c *Client) UpdateTodo(ctx context.Context, id int, input UpdateTodo) (Todo, error) {
	return httpx.ToJSON[Todo](
		c.PUT("/todos/{id}").
			PathParam("id", fmt.Sprint(id)).
			JSON(input).
			Do(ctx),
	)
}

// PatchTodo calls PATCH /todos/{id} (partial update).
func (c *Client) PatchTodo(ctx context.Context, id int, input PatchTodo) (Todo, error) {
	return httpx.ToJSON[Todo](
		c.PATCH("/todos/{id}").
			PathParam("id", fmt.Sprint(id)).
			JSON(input).
			Do(ctx),
	)
}

// DeleteTodo calls DELETE /todos/{id}.
func (c *Client) DeleteTodo(ctx context.Context, id int) error {
	resp, err := c.DELETE("/todos/{id}").PathParam("id", fmt.Sprint(id)).Do(ctx)
	if err != nil {
		return err
	}
	defer resp.Close()

	if !resp.IsSuccess() {
		return fmt.Errorf("delete todo failed: HTTP %s", resp.Status)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds the demo so deferred cleanup (cancel) always executes;
// log.Fatal in main would skip defers.
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient()

	// 1. LIST
	section("LIST TODOS")

	todos, err := client.ListTodos(ctx, exampleUser)
	if err != nil {
		return fmt.Errorf("ListTodos: %w", err)
	}
	for _, t := range todos {
		fmt.Printf("ID: %d | User: %d | Title: %s | Completed: %t\n",
			t.ID, t.UserID, t.Title, t.Completed)
	}

	// 2. GET
	section("GET TODO")

	todo, err := client.GetTodo(ctx, exampleID)
	if err != nil {
		return fmt.Errorf("GetTodo: %w", err)
	}
	fmt.Printf("%+v\n", todo)

	// 3. CREATE
	section("CREATE TODO")

	created, err := client.CreateTodo(ctx, CreateTodo{
		UserID:    exampleUser,
		Title:     "Learn Go HTTP client",
		Completed: false,
	})
	if err != nil {
		return fmt.Errorf("CreateTodo: %w", err)
	}
	fmt.Printf("Created: %+v\n", created)

	// 4. UPDATE (PUT: complete replacement)
	section("UPDATE TODO")

	updated, err := client.UpdateTodo(ctx, exampleID, UpdateTodo{
		ID:        exampleID,
		UserID:    exampleUser,
		Title:     "Learn Go and REST APIs",
		Completed: true,
	})
	if err != nil {
		return fmt.Errorf("UpdateTodo: %w", err)
	}
	fmt.Printf("Updated: %+v\n", updated)

	// 5. PATCH (partial update)
	section("PATCH TODO")

	newTitle := "Learn Go with httpx"
	completed := false

	patched, err := client.PatchTodo(ctx, exampleID, PatchTodo{
		Title:     &newTitle,
		Completed: &completed,
	})
	if err != nil {
		return fmt.Errorf("PatchTodo: %w", err)
	}
	fmt.Printf("Patched: %+v\n", patched)

	// 6. DELETE
	section("DELETE TODO")

	if err := client.DeleteTodo(ctx, exampleID); err != nil {
		return fmt.Errorf("DeleteTodo: %w", err)
	}
	fmt.Println("Todo deleted successfully")

	return nil
}

// section prints a banner so each step is easy to find in the output.
func section(title string) {
	fmt.Printf("\n========== %s ==========\n", title)
}
