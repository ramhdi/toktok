// signaling/room.go
package signaling

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

type Room struct {
	ID              string
	broadcaster     *websocket.Conn
	viewers         map[*websocket.Conn]bool
	localTrack      *webrtc.TrackLocalStaticRTP
	BroadcasterChan chan string // For SDP signaling
	ViewerChan      chan string // For SDP signaling
	mu              sync.RWMutex
	createdAt       time.Time
	isActive        bool
}

func NewRoom(id string) *Room {
	return &Room{
		ID:              id,
		viewers:         make(map[*websocket.Conn]bool),
		BroadcasterChan: make(chan string),
		ViewerChan:      make(chan string),
		createdAt:       time.Now(),
		isActive:        true,
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

	delete(r.viewers, conn)
}

func (r *Room) SetLocalTrack(track *webrtc.TrackLocalStaticRTP) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.localTrack = track
}

func (r *Room) GetLocalTrack() *webrtc.TrackLocalStaticRTP {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.localTrack
}

func (r *Room) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isActive {
		return
	}

	r.isActive = false

	// Notify all viewers that the room is closing
	for viewer := range r.viewers {
		sendMessage(viewer, Message{
			Type: "room-closed",
			SDP:  "Broadcaster has left the room",
		})
	}

	// Clear viewers
	r.viewers = make(map[*websocket.Conn]bool)

	// Close channels
	close(r.BroadcasterChan)
	close(r.ViewerChan)

	log.Printf("Room %s closed", r.ID)
}

func (r *Room) IsActive() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isActive
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

func (rm *RoomManager) CreateRoom(id string) (*Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.rooms[id]; exists {
		return nil, fmt.Errorf("room %s already exists", id)
	}

	room := NewRoom(id)
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

// For the initial single-room implementation
func (rm *RoomManager) GetDefaultRoom() (*Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Create default room if it doesn't exist
	const defaultRoomID = "default"
	if room, exists := rm.rooms[defaultRoomID]; exists {
		return room, nil
	}

	room := NewRoom(defaultRoomID)
	rm.rooms[defaultRoomID] = room
	return room, nil
}
