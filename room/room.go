// room/room.go
package room

import (
	"fmt"
	"log"
	"sync"
	"time"
	"toktok/stream"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

type RoomState int

const (
	Created RoomState = iota
	PendingBroadcast
	Broadcasting
	Closing
	Closed
)

type Room struct {
	ID        string
	state     RoomState
	createdAt time.Time

	// WebRTC components
	broadcaster *stream.Broadcaster
	viewers     map[string]*stream.Viewer // key: viewerID
	localTrack  *webrtc.TrackLocalStaticRTP

	// WebSocket connections
	broadcasterConn *websocket.Conn
	viewerConns     map[string]*websocket.Conn // key: viewerID

	// Signaling channels
	broadcasterSignal chan string
	viewerSignals     map[string]chan string // key: viewerID

	// Synchronization
	mu sync.RWMutex

	// Configuration
	config *Config
}

type Config struct {
	StunURL     string
	PLIInterval int
}

func NewRoom(id string, config *Config) *Room {
	return &Room{
		ID:                id,
		state:             Created,
		createdAt:         time.Now(),
		viewers:           make(map[string]*stream.Viewer),
		viewerConns:       make(map[string]*websocket.Conn),
		viewerSignals:     make(map[string]chan string),
		broadcasterSignal: make(chan string),
		config:            config,
	}
}

// RegisterBroadcaster handles broadcaster registration and WebRTC setup
func (r *Room) RegisterBroadcaster(conn *websocket.Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Created {
		return fmt.Errorf("room %s already has a broadcaster", r.ID)
	}

	var err error
	r.broadcaster, err = stream.NewBroadcaster(r.config.StunURL, r.config.PLIInterval)
	if err != nil {
		return fmt.Errorf("failed to create broadcaster: %w", err)
	}

	r.broadcasterConn = conn
	r.state = PendingBroadcast
	return nil
}

// HandleBroadcasterOffer processes the SDP offer from broadcaster
func (r *Room) HandleBroadcasterOffer(sdp string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != PendingBroadcast {
		return "", fmt.Errorf("room %s not ready for broadcast", r.ID)
	}

	// Start WebRTC connection
	answer, err := r.broadcaster.Start(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	})
	if err != nil {
		return "", fmt.Errorf("failed to start broadcast: %w", err)
	}

	// Get the local track for viewers
	r.localTrack, err = r.broadcaster.GetLocalTrack()
	if err != nil {
		return "", fmt.Errorf("failed to get local track: %w", err)
	}

	r.state = Broadcasting
	return answer, nil
}

// RegisterViewer handles viewer registration
func (r *Room) RegisterViewer(viewerID string, conn *websocket.Conn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Broadcasting {
		return fmt.Errorf("room %s is not broadcasting", r.ID)
	}

	if _, exists := r.viewers[viewerID]; exists {
		return fmt.Errorf("viewer %s already exists in room %s", viewerID, r.ID)
	}

	viewer := stream.NewViewer(r.config.StunURL)
	r.viewers[viewerID] = viewer
	r.viewerConns[viewerID] = conn
	r.viewerSignals[viewerID] = make(chan string)
	return nil
}

// HandleViewerOffer processes the SDP offer from a viewer
func (r *Room) HandleViewerOffer(viewerID string, sdp string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	viewer, exists := r.viewers[viewerID]
	if !exists {
		return "", fmt.Errorf("viewer %s not registered in room %s", viewerID, r.ID)
	}

	if r.localTrack == nil {
		return "", fmt.Errorf("no broadcast track available")
	}

	answer, err := viewer.Start(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	}, r.localTrack)
	if err != nil {
		return "", fmt.Errorf("failed to start viewer: %w", err)
	}

	return answer, nil
}

// RemoveViewer removes a viewer from the room
func (r *Room) RemoveViewer(viewerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if viewer, exists := r.viewers[viewerID]; exists {
		viewer.Close()
		delete(r.viewers, viewerID)
		delete(r.viewerConns, viewerID)
		close(r.viewerSignals[viewerID])
		delete(r.viewerSignals, viewerID)
	}
}

// Close ends the broadcast and closes all connections
func (r *Room) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Closed {
		return
	}

	r.state = Closing

	// Close broadcaster
	if r.broadcaster != nil {
		r.broadcaster.Close()
	}

	// Notify and close all viewers
	for viewerID, conn := range r.viewerConns {
		// Send room closed message
		msg := struct {
			Type   string `json:"type"`
			RoomID string `json:"roomId"`
		}{
			Type:   "room-closed",
			RoomID: r.ID,
		}
		conn.WriteJSON(msg)

		// Close viewer
		if viewer, exists := r.viewers[viewerID]; exists {
			viewer.Close()
		}
	}

	// Clear all maps
	r.viewers = make(map[string]*stream.Viewer)
	r.viewerConns = make(map[string]*websocket.Conn)
	for _, ch := range r.viewerSignals {
		close(ch)
	}
	r.viewerSignals = make(map[string]chan string)

	r.state = Closed
}

// HandleBroadcasterDisconnect handles broadcaster disconnection
func (r *Room) HandleBroadcasterDisconnect() {
	log.Printf("Broadcaster disconnected from room %s", r.ID)
	r.Close()
}

// GetState returns current room state
func (r *Room) GetState() RoomState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// GetViewerCount returns current number of viewers
func (r *Room) GetViewerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.viewers)
}

// IsBroadcasterConn checks if the given connection is the broadcaster
func (r *Room) IsBroadcasterConn(conn *websocket.Conn) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return conn == r.broadcasterConn
}
