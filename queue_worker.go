package main

import (
	"context"
	"encoding/json"
	"fmt"
)

type JobStage string

const (
	Checkout    JobStage = "checkout"
	Fulfillment JobStage = "fulfillment"
	Receipt     JobStage = "receipt"
	OrderUpdate JobStage = "customer_order_update"
)

type FailedJob struct {
	OrderID string   `json:"order_id"`
	Stage   JobStage `json:"stage"`
	Attempt int      `json:"attempt"`
	Reason  string   `json:"reason"`
}

type disposition string

const (
	retryJob disposition = "retry"
	dropJob  disposition = "drop"
)

func decide(job FailedJob, maxAttempts int) disposition {
	if job.Attempt < maxAttempts {
		return retryJob
	}
	return dropJob
}

type queueAPI interface {
	Publish(context.Context, FailedJob) error
	Consume(context.Context, int, int) ([]consumedMessage, error)
	Ack(context.Context, string) error
}

type QueueWorker struct {
	queue       queueAPI
	maxAttempts int
}

func (w QueueWorker) Replay(ctx context.Context) (int, error) {
	messages, err := w.queue.Consume(ctx, 10, 30)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, message := range messages {
		var job FailedJob
		if err := json.Unmarshal(message.Payload, &job); err != nil {
			return processed, fmt.Errorf("decode message %s: %w", message.MessageID, err)
		}
		if decide(job, w.maxAttempts) == retryJob {
			job.Attempt++
			if err := w.queue.Publish(ctx, job); err != nil {
				return processed, err
			}
		}
		if err := w.queue.Ack(ctx, message.MessageID); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}
