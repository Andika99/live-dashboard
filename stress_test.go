package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	targetURL    = "http://localhost:8080/stream"
	totalClients = 1000          
	testDuration = 5 * time.Second 
)

// Changed from func main() to a standard Go Test function
func TestServerStress(t *testing.T) {
	var activeConnections int64
	var totalMessagesReceived int64
	var connectionFailures int64

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	fmt.Printf("\n⚡ Starting stress test: Spawning %d concurrent SSE clients...\n", totalClients)
	startTime := time.Now()

	for i := 0; i < totalClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
			if err != nil {
				atomic.AddInt64(&connectionFailures, 1)
				return
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				atomic.AddInt64(&connectionFailures, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				atomic.AddInt64(&connectionFailures, 1)
				return
			}

			atomic.AddInt64(&activeConnections, 1)

			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				line := scanner.Text()
				if line != "" && len(line) >= 5 && line[:5] == "data:" {
					atomic.AddInt64(&totalMessagesReceived, 1)
				}
			}
		}(i)
		
		time.Sleep(1 * time.Millisecond) 
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Println("\n📊 --- STRESS TEST RESULTS ---")
	fmt.Printf("Duration:               %v\n", duration)
	fmt.Printf("Peak Concurrent Users:  %d / %d\n", activeConnections, totalClients)
	fmt.Printf("Connection Failures:    %d\n", connectionFailures)
	fmt.Printf("Total Payloads Read:    %d\n", totalMessagesReceived)
	fmt.Printf("Throughput:             %.2f events/sec\n", float64(totalMessagesReceived)/duration.Seconds())
}
