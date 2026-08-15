package main

import (
	"sync"
)

// Broker maintains the set of active clients and broadcasts messages to them.
type Broker struct {
	// Guard access to the clients map
	mu sync.Mutex

	// Map of active client channels. The boolean value is just a placeholder.
	clients map[chan string]bool

	// Inbound messages from the data generator
	incoming chan string
}

// NewBroker initializes a new thread-safe message broker.
func NewBroker() *Broker {
	return &Broker{
		clients:  make(map[chan string]bool),
		incoming: make(chan string, 100), // Buffered to handle sudden bursts
	}
}

// Register adds a new client channel to the active registry.
func (b *Broker) Register(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[ch] = true
}

// Unregister safely removes and closes a client channel to prevent memory leaks.
func (b *Broker) Unregister(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.clients[ch]; exists {
		delete(b.clients, ch)
		close(ch)
	}
}

// Start runs an infinite loop processing inbound data and broadcasting it to all active web clients.
func (b *Broker) Start() {
	for msg := range b.incoming {
		b.mu.Lock()
		// Broadcast the message to every connected client channel
		for ch := range b.clients {
			select {
			case ch <- msg:
				// Message sent successfully
			default:
				// Slow client dropped the message to prevent blocking the entire server
			}
		}
		b.mu.Unlock()
	}
}
