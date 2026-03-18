package utils

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type mockHTTPClient struct {
	response *http.Response
	err      error
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.response, m.err
}

func newMockResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// TestDoGet tests HTTP GET request handling.
func TestDoGet(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		client := &mockHTTPClient{
			response: newMockResponse(http.StatusOK, `{"price": 100}`),
		}

		resp, err := DoGet(context.Background(), client, "https://api.example.com/price", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	})

	t.Run("non-success status returns error", func(t *testing.T) {
		tests := []int{400, 401, 404, 429, 500}
		for _, status := range tests {
			client := &mockHTTPClient{
				response: newMockResponse(status, "error"),
			}

			_, err := DoGet(context.Background(), client, "https://api.example.com/test", nil)
			if err == nil {
				t.Errorf("expected error for status %d", status)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("%d", status)) {
				t.Errorf("error for status %d missing status code: %v", status, err)
			}
		}
	})

	t.Run("error body included in message", func(t *testing.T) {
		errorMsg := "Invalid API key"
		client := &mockHTTPClient{
			response: newMockResponse(http.StatusUnauthorized, errorMsg),
		}

		_, err := DoGet(context.Background(), client, "https://api.example.com/test", nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), errorMsg) {
			t.Errorf("error should include response body: %v", err)
		}
	})

	t.Run("network error propagated", func(t *testing.T) {
		client := &mockHTTPClient{
			err: fmt.Errorf("connection refused"),
		}

		_, err := DoGet(context.Background(), client, "https://api.example.com/test", nil)
		if err == nil {
			t.Fatal("expected network error")
		}
		if !strings.Contains(err.Error(), "connection refused") {
			t.Errorf("error should contain network error: %v", err)
		}
	})
}

// TestStreamDecodeJSON tests JSON stream decoding.
func TestStreamDecodeJSON(t *testing.T) {
	t.Run("valid json decodes successfully", func(t *testing.T) {
		body := `{"price": 100}`
		reader := strings.NewReader(body)

		var result map[string]interface{}
		err := StreamDecodeJSON(reader, &result)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result["price"] != float64(100) {
			t.Errorf("expected price 100, got %v", result["price"])
		}
	})

	t.Run("trailing json detected", func(t *testing.T) {
		body := `{"valid": true}{"extra": "data"}`
		reader := strings.NewReader(body)

		var result map[string]interface{}
		err := StreamDecodeJSON(reader, &result)
		if err == nil {
			t.Fatal("expected error for trailing JSON")
		}
	})

	t.Run("invalid json returns error", func(t *testing.T) {
		tests := []string{
			`{invalid}`,
			`{"unclosed": "string}`,
			`garbage`,
		}

		for _, body := range tests {
			reader := strings.NewReader(body)
			var result interface{}
			err := StreamDecodeJSON(reader, &result)
			if err == nil {
				t.Errorf("expected error for invalid json: %s", body)
			}
		}
	})

	t.Run("oversized payload rejected", func(t *testing.T) {
		largeValue := strings.Repeat("x", 11*1024*1024)
		body := fmt.Sprintf(`{"data": "%s"}`, largeValue)
		reader := strings.NewReader(body)

		var result interface{}
		err := StreamDecodeJSON(reader, &result)
		if err == nil {
			t.Fatal("expected error for payload > 10 MiB")
		}
	})

	t.Run("empty input returns error", func(t *testing.T) {
		reader := strings.NewReader("")

		var result interface{}
		err := StreamDecodeJSON(reader, &result)
		if err == nil {
			t.Fatal("expected error for empty input")
		}
	})
}
