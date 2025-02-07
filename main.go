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

	// Handle first connection (broadcaster)
	log.Println("Waiting for broadcaster...")
	offer := webrtc.SessionDescription{}
	if err := utils.Decode(<-sigServer.GetSDPChan(), &offer); err != nil {
		log.Fatalf("Failed to decode offer: %v", err)
	} else {
		log.Printf("Broadcaster offer = %v\n", offer)
	}

	answer, err := broadcaster.Start(offer)
	if err != nil {
		log.Fatalf("Failed to start broadcaster: %v", err)
	}
	log.Println("Answer=")
	log.Println(answer)

	// Handle viewer connections
	log.Println("Waiting for viewers...")
	localTrack, err := broadcaster.GetLocalTrack()
	if err != nil {
		log.Fatalf("Failed to get local track: %v", err)
	}

	go func() {
		for {
			log.Println("\nWaiting for viewer offer...")
			viewerOffer := webrtc.SessionDescription{}
			if err := utils.Decode(<-sigServer.GetSDPChan(), &viewerOffer); err != nil {
				log.Printf("Failed to decode viewer offer: %v", err)
				continue
			} else {
				log.Printf("Viewer offer = %v\n", viewerOffer)
			}

			viewer := stream.NewViewer(cfg.StunURL)
			answer, err := viewer.Start(viewerOffer, localTrack)
			if err != nil {
				log.Printf("Failed to start viewer: %v", err)
				continue
			}
			log.Println(answer)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
