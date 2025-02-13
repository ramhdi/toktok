// main.go
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"toktok/config"
	"toktok/signaling"
	"toktok/stream"
	"toktok/utils"

	"github.com/pion/webrtc/v3"
)

func main() {
	cfg := config.NewConfig()

	// Create and start signaling server
	sigServer := signaling.NewServer(cfg.Port)

	// Create default room
	room, err := sigServer.GetRoomManager().CreateRoom("default")
	if err != nil {
		log.Fatalf("Failed to create default room: %v", err)
	}
	// Set as active room
	sigServer.SetActiveRoom(room)

	// Start server
	go func() {
		if err := sigServer.Start(); err != nil {
			log.Fatalf("Failed to start signaling server: %v", err)
		}
	}()

	// Create broadcaster
	broadcaster, err := stream.NewBroadcaster(cfg.StunURL, cfg.PLIInterval)
	if err != nil {
		log.Fatalf("Failed to create broadcaster: %v", err)
	}
	defer broadcaster.Close()

	// Get channels for SDP communication
	broadcasterSDPChan := sigServer.GetBroadcasterSDPChan()
	viewerSDPChan := sigServer.GetViewerSDPChan()

	// Handle first connection (broadcaster)
	log.Println("Waiting for broadcaster...")
	offer := webrtc.SessionDescription{}
	if err := utils.Decode(<-broadcasterSDPChan, &offer); err != nil {
		log.Fatalf("Failed to decode broadcaster offer: %v", err)
	}

	answer, err := broadcaster.Start(offer)
	if err != nil {
		log.Fatalf("Failed to start broadcaster: %v", err)
	}
	broadcasterSDPChan <- answer // Send answer back through channel

	// Handle viewer connections
	log.Println("Waiting for viewers...")
	localTrack, err := broadcaster.GetLocalTrack()
	if err != nil {
		log.Fatalf("Failed to get local track: %v", err)
	}

	// Handle viewer connections in a separate goroutine
	go func() {
		for {
			log.Println("\nWaiting for viewer offer...")
			viewerOffer := webrtc.SessionDescription{}
			if err := utils.Decode(<-viewerSDPChan, &viewerOffer); err != nil {
				log.Printf("Failed to decode viewer offer: %v", err)
				continue
			}

			viewer := stream.NewViewer(cfg.StunURL)
			answer, err := viewer.Start(viewerOffer, localTrack)
			if err != nil {
				log.Printf("Failed to start viewer: %v", err)
				continue
			}
			viewerSDPChan <- answer // Send answer back through channel
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	sigServer.GetRoomManager().DeleteRoom("default")
}
