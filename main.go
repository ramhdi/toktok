// main.go
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"toktok/config"
	"toktok/room"
	"toktok/signaling"
)

func main() {
	cfg := config.NewConfig()

	// Create room config
	roomConfig := &room.Config{
		StunURL:     cfg.StunURL,
		PLIInterval: cfg.PLIInterval,
	}

	// Create default room (to maintain current behavior)
	defaultRoom := room.NewRoom("default", roomConfig)

	// Create and start signaling server with the default room
	sigServer := signaling.NewServer(cfg.Port, defaultRoom)
	go func() {
		if err := sigServer.Start(); err != nil {
			log.Fatalf("Failed to start signaling server: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Clean up
	defaultRoom.Close()
}
