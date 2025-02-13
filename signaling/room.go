// signaling/room.go
package signaling

import (
	"fmt"
	"log"
	"sync"
	"time"
	"toktok/stream"
	"toktok/utils"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

type Room struct {
	ID          string
	broadcaster *websocket.Conn
	viewers     map[*websocket.Conn]bool
	// WebRTC components
	broadcasterPC *stream.Broadcaster
	viewerPCs     map[*websocket.Conn]*stream.Viewer
	localTrack    *webrtc.TrackLocalStaticRTP
	// Signaling channels
	BroadcasterChan chan string
	ViewerChan      chan string
	// Configuration
	stunURL string
	// Synchronization
	mu        sync.RWMutex
	createdAt time.Time
	isActive  bool
}

func NewRoom(id string, stunURL string, pliInterval int) (*Room, error) {
	// Create broadcaster
	broadcaster, err := stream.NewBroadcaster(stunURL, pliInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create broadcaster: %v", err)
	}

	return &Room{
		ID:              id,
		viewers:         make(map[*websocket.Conn]bool),
		viewerPCs:       make(map[*websocket.Conn]*stream.Viewer),
		broadcasterPC:   broadcaster,
		BroadcasterChan: make(chan string),
		ViewerChan:      make(chan string),
		stunURL:         stunURL,
		createdAt:       time.Now(),
		isActive:        true,
	}, nil
}

func (r *Room) Start() {
	// Channel to signal when broadcaster is ready
	broadcasterReady := make(chan struct{})

	// Start broadcaster handler
	go func() {
		r.handleBroadcaster()
		close(broadcasterReady)
	}()

	// Start viewer handler after broadcaster is ready
	go func() {
		<-broadcasterReady
		r.handleViewers()
	}()
}

func (r *Room) handleBroadcaster() {
	// Wait for broadcaster offer
	log.Printf("Room %s: Waiting for broadcaster...", r.ID)
	offer := webrtc.SessionDescription{}
	if err := utils.Decode(<-r.BroadcasterChan, &offer); err != nil {
		log.Printf("Failed to decode broadcaster offer: %v", err)
		return
	}

	// Start WebRTC connection
	answer, err := r.broadcasterPC.Start(offer)
	if err != nil {
		log.Printf("Failed to start broadcaster: %v", err)
		return
	}
	r.BroadcasterChan <- answer

	// Get and store local track
	r.localTrack, err = r.broadcasterPC.GetLocalTrack()
	if err != nil {
		log.Printf("Failed to get local track: %v", err)
		return
	}
}

func (r *Room) handleViewers() {
	for {
		if !r.isActive {
			return
		}

		log.Printf("Room %s: Waiting for viewer offer...", r.ID)
		viewerOffer := webrtc.SessionDescription{}
		if err := utils.Decode(<-r.ViewerChan, &viewerOffer); err != nil {
			log.Printf("Failed to decode viewer offer: %v", err)
			continue
		}

		viewer := stream.NewViewer(r.stunURL)
		answer, err := viewer.Start(viewerOffer, r.localTrack)
		if err != nil {
			log.Printf("Failed to start viewer: %v", err)
			continue
		}
		r.ViewerChan <- answer
	}
}

func (r *Room) SetBroadcaster(conn *websocket.Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.broadcaster != nil {
		return fmt.Errorf("broadcaster already exists")
	}

	r.broadcaster = conn
	return nil
}

func (r *Room) AddViewer(conn *websocket.Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isActive {
		return fmt.Errorf("room is not active")
	}

	if r.broadcaster == nil {
		return fmt.Errorf("no broadcaster in room")
	}

	r.viewers[conn] = true
	return nil
}

func (r *Room) RemoveViewer(conn *websocket.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if viewer, exists := r.viewerPCs[conn]; exists {
		viewer.Close()
		delete(r.viewerPCs, conn)
	}
	delete(r.viewers, conn)
}

func (r *Room) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isActive {
		return
	}

	r.isActive = false

	// Close broadcaster
	if r.broadcasterPC != nil {
		r.broadcasterPC.Close()
	}

	// Notify and close all viewers
	for viewer := range r.viewers {
		sendMessage(viewer, Message{
			Type: "room-closed",
			SDP:  "Broadcaster has left the room",
		})
	}

	// Close all viewer connections
	for _, viewer := range r.viewerPCs {
		viewer.Close()
	}

	// Clear maps
	r.viewers = make(map[*websocket.Conn]bool)
	r.viewerPCs = make(map[*websocket.Conn]*stream.Viewer)

	// Close channels
	close(r.BroadcasterChan)
	close(r.ViewerChan)

	log.Printf("Room %s closed", r.ID)
}

type RoomManager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*Room),
	}
}

func (rm *RoomManager) CreateRoom(id string, stunURL string, pliInterval int) (*Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.rooms[id]; exists {
		return nil, fmt.Errorf("room %s already exists", id)
	}

	room, err := NewRoom(id, stunURL, pliInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create room: %w", err)
	}

	rm.rooms[id] = room
	return room, nil
}

func (rm *RoomManager) GetRoom(id string) (*Room, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	room, exists := rm.rooms[id]
	if !exists {
		return nil, fmt.Errorf("room %s not found", id)
	}

	return room, nil
}

func (rm *RoomManager) DeleteRoom(id string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if room, exists := rm.rooms[id]; exists {
		room.Close()
		delete(rm.rooms, id)
	}
}

func (rm *RoomManager) ListRooms() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rooms := make([]string, 0, len(rm.rooms))
	for id := range rm.rooms {
		rooms = append(rooms, id)
	}
	return rooms
}
