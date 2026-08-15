package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

// Metrics represents the live dashboard data payload
type Metrics struct {
	Timestamp string  `json:"timestamp"`
	CPUUsage  float64 `json:"cpu_usage"`
	MemUsage  float64 `json:"mem_usage"`
	Requests  int     `json:"requests"`
}

// generateMockData acts as our streaming ingestion source.
// It generates random metrics every 200ms and sends them to the broker.
func generateMockData(broker *Broker) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		data := Metrics{
			Timestamp: time.Now().Format("15:04:05.000"),
			CPUUsage:  20.0 + rand.Float64()*40.0, // 20% - 60%
			MemUsage:  45.0 + rand.Float64()*15.0, // 45% - 60%
			Requests:  rand.Intn(150) + 50,        // 50 - 200 reqs
		}

		payload, err := json.Marshal(data)
		if err != nil {
			log.Printf("JSON marshaling failed: %v", err)
			continue
		}

		broker.incoming <- string(payload)
	}
}

func main() {
	broker := NewBroker()

	// Start background engine loops
	go broker.Start()
	go generateMockData(broker)

	// Serve the static HTML frontend dashboard
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// Server-Sent Events (SSE) Streaming Endpoint
	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		// 1. Assert that our HTTP response writer supports flushing data mid-request
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported by network framework", http.StatusInternalServerError)
			return
		}

		// 2. Set the standard HTTP headers required for persistent SSE streams
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// 3. Create a unique consumer channel for this specific browser tab connection
		clientChan := make(chan string, 10)
		broker.Register(clientChan)

		// 4. Ensure cleanup occurs the millisecond the handler exits
		defer broker.Unregister(clientChan)

		// 5. Enter infinite select loop waiting for network events
		for {
			select {
			case msg, open := <-clientChan:
				if !open {
					return // Channel closed by broker
				}
				// SSE protocol requires data to look exactly like: "data: <content>\n\n"
				fmt.Fprintf(w, "data: %s\n\n", msg)
				flusher.Flush() // Force immediate transfer down the network wire

			case <-r.Context().Done():
				// CRITICAL FOR RECRUITERS: Client closed browser tab, context cancelled.
				// Exiting here triggers our deferred unregister, stopping memory leaks entirely.
				log.Println("Web client disconnected. Resources reclaimed safely.")
				return
			}
		}
	})

	log.Println("🚀 Dashboard engine live at http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server crash: %v", err)
	}
}
