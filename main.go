// main.go
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"toktok/config"
	"toktok/signaling"
)

func main() {
	cfg := config.NewConfig()

	// Create signaling server
	sigServer := signaling.NewServer(cfg.Port)

	// Start server first
	go func() {
		if err := sigServer.Start(); err != nil {
			log.Fatalf("Failed to start signaling server: %v", err)
		}
	}()
	log.Printf("Starting server on :%d", cfg.Port)

	// Create default room after server is started
	room, err := sigServer.GetRoomManager().CreateRoom("default", cfg.StunURL, cfg.PLIInterval)
	if err != nil {
		log.Fatalf("Failed to create default room: %v", err)
	}
	log.Printf("Created default room")

	// Start room handling after room is created
	room.Start()
	log.Printf("Room started and ready for connections")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	sigServer.GetRoomManager().DeleteRoom("default")
}
