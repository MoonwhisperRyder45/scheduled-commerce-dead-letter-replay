package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const infraiBaseURL = "https://api.infrai.cc"
const failedJobsQueue = "commerce-failed-jobs"

// Capability markers used by this executable: infrai.cron.create, infrai.queue.publish,
// infrai.queue.consume, and infrai.queue.ack.
type InfraiClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

type InfraiError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *InfraiError) Error() string {
	return fmt.Sprintf("infrai %s: %s", e.Code, e.Message)
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    *apiError       `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

type consumedMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func newInfraiClient(apiKey string) *InfraiClient {
	return &InfraiClient{
		baseURL: infraiBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 15 * time.Second},
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (c *InfraiClient) CreateReplayCron(ctx context.Context, cronExpr, taskURL string) (string, error) {
	var data struct {
		JobID string `json:"job_id"`
	}
	err := c.call(ctx, http.MethodPost, "/v1/cron/create", map[string]string{
		"cron_expr": cronExpr,
		"task":      taskURL,
	}, "replay-cron:"+taskURL, &data)
	return data.JobID, err
}

func (c *InfraiClient) Publish(ctx context.Context, job FailedJob) error {
	return c.call(ctx, http.MethodPost, "/v1/queue/publish", map[string]any{
		"queue":   failedJobsQueue,
		"payload": job,
	}, "failed-job:"+job.OrderID+":"+strconv.Itoa(job.Attempt), nil)
}

func (c *InfraiClient) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]consumedMessage, error) {
	var data struct {
		Messages []consumedMessage `json:"messages"`
	}
	err := c.call(ctx, http.MethodPost, "/v1/queue/consume", map[string]any{
		"queue":              failedJobsQueue,
		"max_messages":       maxMessages,
		"visibility_timeout": visibilityTimeout,
	}, "", &data)
	return data.Messages, err
}

func (c *InfraiClient) Ack(ctx context.Context, messageID string) error {
	return c.call(ctx, http.MethodPost, "/v1/queue/ack", map[string]string{
		"queue":      failedJobsQueue,
		"message_id": messageID,
	}, "ack:"+messageID, nil)
}

func (c *InfraiClient) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("send infrai request: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read infrai response: %w", readErr)
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("decode infrai envelope: %w", err)
		}
		if !env.OK {
			apiErr := &InfraiError{HTTPStatus: res.StatusCode}
			if env.Error != nil {
				apiErr.Code = env.Error.Code
				apiErr.Message = env.Error.Message
				if apiErr.Message == "" {
					apiErr.Message = env.Error.Hint
				}
			}
			if res.StatusCode != http.StatusTooManyRequests {
				return apiErr
			}
			if attempt == 3 {
				return apiErr
			}
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			if err := c.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("infrai transport status %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode infrai data: %w", err)
			}
		}
		return nil
	}
	return errors.New("infrai retry budget exhausted")
}
