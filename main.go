package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	publicURL := os.Getenv("PUBLIC_URL")
	if publicURL == "" {
		log.Fatal("PUBLIC_URL is required")
	}

	client := newInfraiClient(apiKey)
	worker := QueueWorker{queue: client, maxAttempts: 3}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var job FailedJob
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
			http.Error(w, "invalid job", http.StatusBadRequest)
			return
		}
		if job.OrderID == "" || !validStage(job.Stage) {
			http.Error(w, "order_id and valid stage are required", http.StatusBadRequest)
			return
		}
		if err := client.Publish(r.Context(), job); err != nil {
			writeUpstreamError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "queued", "order_id": job.OrderID})
	})
	mux.HandleFunc("POST /replay", func(w http.ResponseWriter, r *http.Request) {
		count, err := worker.Replay(r.Context())
		if err != nil {
			writeUpstreamError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "replayed", "processed": count})
	})
	mux.HandleFunc("POST /setup", func(w http.ResponseWriter, r *http.Request) {
		jobID, err := client.CreateReplayCron(r.Context(), "*/5 * * * *", publicURL+"/replay")
		if err != nil {
			writeUpstreamError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"status": "scheduled", "job_id": jobID})
	})

	log.Printf("dead-letter service listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func validStage(stage JobStage) bool {
	return stage == Checkout || stage == Fulfillment || stage == Receipt || stage == OrderUpdate
}

func writeUpstreamError(w http.ResponseWriter, err error) {
	var apiErr *InfraiError
	if errors.As(err, &apiErr) && apiErr.HTTPStatus >= 400 && apiErr.HTTPStatus < 500 {
		writeJSON(w, apiErr.HTTPStatus, map[string]string{"error": apiErr.Code, "message": apiErr.Message})
		return
	}
	http.Error(w, "queue operation failed", http.StatusBadGateway)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
