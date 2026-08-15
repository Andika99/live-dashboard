package main

import (
	"encoding/json"
	"fmt"
	"log"
	// "math/rand"
	"net/http"
	"time"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// Metrics structure remains matching your frontend names
type Metrics struct {
	Timestamp string  `json:"timestamp"`
	CPUUsage  float64 `json:"cpu_usage"`
	MemUsage  float64 `json:"mem_usage"`
	Requests  int     `json:"requests"` // We can use this to show total memory used in MB instead
}

func generateRealData(broker *Broker) {
	// Query the hardware every 500ms to avoid overwhelming the OS kernel
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// 1. Fetch Real CPU Utilization 
		// Passing 0 tells it to calculate the average usage since the last check immediately
		cpuPercentages, err := cpu.Percent(0, false)
		var currentCPU float64
		if err == nil && len(cpuPercentages) > 0 {
			currentCPU = cpuPercentages[0]
		}

		// 2. Fetch Real Virtual Memory Allocation
		vMem, err := mem.VirtualMemory()
		var currentMem float64
		var totalUsedMB int
		if err == nil {
			currentMem = vMem.UsedPercent
			// Convert bytes used to Megabytes for an extra cool live data point
			totalUsedMB = int(vMem.Used / 1024 / 1024) 
		}

		// 3. Assemble the authentic system payload
		data := Metrics{
			Timestamp: time.Now().Format("15:04:05.000"),
			CPUUsage:  currentCPU,
			MemUsage:  currentMem,
			Requests:  totalUsedMB, // Swapping mock requests with real MB allocated
		}

		payload, err := json.Marshal(data)
		if err != nil {
			log.Printf("JSON marshaling failed: %v", err)
			continue
		}

		// Push directly into your streaming broker pipeline
		broker.incoming <- string(payload)
	}
}


func main() {
	broker := NewBroker()

	// Start background engine loops
	go broker.Start()
	go generateRealData(broker)

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
