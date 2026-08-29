package main

import "testing"

func TestDecideFailedJob(t *testing.T) {
	tests := []struct {
		name        string
		job         FailedJob
		maxAttempts int
		want        disposition
	}{
		{name: "checkout gets another attempt", job: FailedJob{OrderID: "ord-101", Stage: Checkout, Attempt: 1}, maxAttempts: 3, want: retryJob},
		{name: "fulfillment poison job is closed", job: FailedJob{OrderID: "ord-102", Stage: Fulfillment, Attempt: 3}, maxAttempts: 3, want: dropJob},
		{name: "receipt at boundary is closed", job: FailedJob{OrderID: "ord-103", Stage: Receipt, Attempt: 2}, maxAttempts: 2, want: dropJob},
		{name: "order update below boundary retries", job: FailedJob{OrderID: "ord-104", Stage: OrderUpdate, Attempt: 0}, maxAttempts: 1, want: retryJob},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decide(tt.job, tt.maxAttempts); got != tt.want {
				t.Fatalf("decide() = %q, want %q", got, tt.want)
			}
		})
	}
}
