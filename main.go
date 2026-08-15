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
	"github.com/shirou/gopsutil/v3/disk"
)

// Metrics structure with read/write speed (bytes per second)
type Metrics struct {
	Timestamp      string  `json:"timestamp"`
	CPUUsage       float64 `json:"cpu_usage"`
	MemUsage       float64 `json:"mem_usage"`
	DiskUsage      float64 `json:"disk_usage"`
	DiskWrite      uint64  `json:"disk_write"`      // Total bytes written (cumulative)
	DiskRead       uint64  `json:"disk_read"`       // Total bytes read (cumulative)
	DiskWriteSpeed uint64  `json:"disk_write_speed"` // Bytes per second
	DiskReadSpeed  uint64  `json:"disk_read_speed"`  // Bytes per second
}

// Track previous values to calculate speed
type DiskStats struct {
	PrevWrite uint64
	PrevRead  uint64
	PrevTime  time.Time
}

func generateRealData(broker *Broker) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Initialize disk stats tracker
	diskStats := DiskStats{
		PrevWrite: 0,
		PrevRead:  0,
		PrevTime:  time.Now(),
	}

	for range ticker.C {
		// 1. Fetch Real CPU Utilization 
		cpuPercentages, err := cpu.Percent(0, false)
		var currentCPU float64
		if err == nil && len(cpuPercentages) > 0 {
			currentCPU = cpuPercentages[0]
		}

		// 2. Fetch Real Virtual Memory Allocation
		vMem, err := mem.VirtualMemory()
		var currentMem float64
		if err == nil {
			currentMem = vMem.UsedPercent
		}

		// 3. Check Disk Percentage
		diskUsage, err := disk.Usage("/")
		var currentDisk float64
		if err == nil {
			currentDisk = diskUsage.UsedPercent
		}

		// 4. Get Disk I/O Statistics
		ioStats, err := disk.IOCounters()
		var diskWrite uint64
		var diskRead uint64
		var diskWriteSpeed uint64
		var diskReadSpeed uint64
		
		if err == nil {
			// Try common disk names
			stat, ok := ioStats["sda"]
			if !ok {
				stat, ok = ioStats["nvme0n1"]
			}
			if !ok {
				stat, ok = ioStats["vda"]
			}
			if !ok {
				// Fallback to first available
				for _, s := range ioStats {
					stat = s
					break
				}
			}
			
			diskWrite = stat.WriteBytes
			diskRead = stat.ReadBytes

			// Calculate speed (bytes per second)
			now := time.Now()
			timeDelta := now.Sub(diskStats.PrevTime).Seconds()
			
			if timeDelta > 0 && diskStats.PrevWrite > 0 && diskStats.PrevRead > 0 {
				// Only calculate if we have previous values
				writeDelta := diskWrite - diskStats.PrevWrite
				readDelta := diskRead - diskStats.PrevRead
				
				// Speed = bytes / seconds
				diskWriteSpeed = uint64(float64(writeDelta) / timeDelta)
				diskReadSpeed = uint64(float64(readDelta) / timeDelta)
			}
			
			// Update previous values for next calculation
			diskStats.PrevWrite = diskWrite
			diskStats.PrevRead = diskRead
			diskStats.PrevTime = now
		}

		// 5. Assemble the payload
		data := Metrics{
			Timestamp:      time.Now().Format("15:04:05.000"),
			CPUUsage:       currentCPU,
			MemUsage:       currentMem,
			DiskUsage:      currentDisk,
			DiskWrite:      diskWrite,
			DiskRead:       diskRead,
			DiskWriteSpeed: diskWriteSpeed,
			DiskReadSpeed:  diskReadSpeed,
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
