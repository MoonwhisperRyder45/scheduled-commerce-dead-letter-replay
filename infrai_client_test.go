package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueueRequestsIncludeQueue(t *testing.T) {
	tests := []struct {
		name string
		path string
		call func(*InfraiClient) error
	}{
		{name: "publish", path: "/v1/queue/publish", call: func(c *InfraiClient) error {
			return c.Publish(context.Background(), FailedJob{OrderID: "ord-1"})
		}},
		{name: "consume", path: "/v1/queue/consume", call: func(c *InfraiClient) error {
			_, err := c.Consume(context.Background(), 10, 30)
			return err
		}},
		{name: "ack", path: "/v1/queue/ack", call: func(c *InfraiClient) error {
			return c.Ack(context.Background(), "msg-1")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tt.path {
					t.Fatalf("path = %q, want %q", r.URL.Path, tt.path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["queue"] != failedJobsQueue {
					t.Errorf("queue = %v, want %q", body["queue"], failedJobsQueue)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true,"data":{"messages":[]}}`))
			}))
			defer server.Close()

			client := newInfraiClient("test-key")
			client.baseURL = server.URL
			if err := tt.call(client); err != nil {
				t.Fatal(err)
			}
		})
	}
}
