// signaling/server.go
package signaling

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"toktok/room"
	"toktok/utils"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

type MessageType string

const (
	// Message types for broadcaster
	BroadcasterRegister     MessageType = "register-broadcaster"
	BroadcasterServerOffer  MessageType = "broadcaster-server-offer"
	ServerBroadcasterAnswer MessageType = "server-broadcaster-answer"

	// Message types for viewer
	ViewerRegister     MessageType = "register-viewer"
	ViewerServerOffer  MessageType = "viewer-server-offer"
	ServerViewerAnswer MessageType = "server-viewer-answer"
)

type Message struct {
	Type MessageType `json:"type"`
	SDP  string      `json:"sdp,omitempty"`
}

type Server struct {
	// WebSocket upgrader
	upgrader websocket.Upgrader

	// Default room (to maintain current behavior)
	room *room.Room

	// Server configuration
	port int

	// Connection tracking
	mu           sync.RWMutex
	viewerID     int
	connToViewer map[*websocket.Conn]string // maps connection to viewerID
}

func NewServer(port int, room *room.Room) *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for testing
			},
		},
		room:         room,
		port:         port,
		connToViewer: make(map[*websocket.Conn]string),
	}
}

func (s *Server) Start() error {
	// Handle static files
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/", fs)

	// Handle WebSocket endpoint
	http.HandleFunc("/ws", s.handleWebSocket)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("Starting server on %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error: %v", err)
			}
			s.handleDisconnect(conn)
			break
		}

		s.handleMessage(conn, &msg)
	}
}

func (s *Server) handleMessage(conn *websocket.Conn, msg *Message) {
	switch msg.Type {
	case BroadcasterRegister:
		s.handleBroadcasterRegister(conn)

	case BroadcasterServerOffer:
		s.handleBroadcasterOffer(conn, msg.SDP)

	case ViewerRegister:
		s.handleViewerRegister(conn)

	case ViewerServerOffer:
		s.handleViewerOffer(conn, msg.SDP)

	default:
		log.Printf("Unknown message type: %s", msg.Type)
	}
}

func (s *Server) handleBroadcasterRegister(conn *websocket.Conn) {
	if err := s.room.RegisterBroadcaster(conn); err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Broadcaster already exists",
		})
		return
	}
	log.Println("Broadcaster registered")
}

func (s *Server) handleBroadcasterOffer(conn *websocket.Conn, sdp string) {
	// Decode the base64-encoded SDP
	var offer webrtc.SessionDescription
	if err := utils.Decode(sdp, &offer); err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  fmt.Sprintf("Failed to decode SDP: %v", err),
		})
		return
	}

	answer, err := s.room.HandleBroadcasterOffer(offer.SDP)
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  fmt.Sprintf("Failed to handle broadcaster offer: %v", err),
		})
		return
	}

	// Encode the answer before sending
	encodedAnswer := utils.Encode(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	})

	sendMessage(conn, Message{
		Type: ServerBroadcasterAnswer,
		SDP:  encodedAnswer,
	})
}

func (s *Server) handleViewerRegister(conn *websocket.Conn) {
	s.mu.Lock()
	viewerID := fmt.Sprintf("viewer-%d", s.viewerID)
	s.viewerID++
	s.connToViewer[conn] = viewerID
	s.mu.Unlock()

	if err := s.room.RegisterViewer(viewerID, conn); err != nil {
		s.mu.Lock()
		delete(s.connToViewer, conn)
		s.mu.Unlock()

		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Failed to register viewer",
		})
		return
	}
	log.Printf("Viewer %s registered", viewerID)
}

func (s *Server) handleViewerOffer(conn *websocket.Conn, sdp string) {
	// Find viewerID for this connection
	s.mu.RLock()
	viewerID := s.findViewerID(conn)
	s.mu.RUnlock()

	if viewerID == "" {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Viewer not registered",
		})
		return
	}

	// Decode the base64-encoded SDP
	var offer webrtc.SessionDescription
	if err := utils.Decode(sdp, &offer); err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  fmt.Sprintf("Failed to decode SDP: %v", err),
		})
		return
	}

	answer, err := s.room.HandleViewerOffer(viewerID, offer.SDP)
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  fmt.Sprintf("Failed to handle viewer offer: %v", err),
		})
		return
	}

	// Encode the answer before sending
	encodedAnswer := utils.Encode(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	})

	sendMessage(conn, Message{
		Type: ServerViewerAnswer,
		SDP:  encodedAnswer,
	})
}

func (s *Server) handleDisconnect(conn *websocket.Conn) {
	// Check if this is the broadcaster
	if s.room.IsBroadcasterConn(conn) {
		s.room.HandleBroadcasterDisconnect()
		return
	}

	// Check if this is a viewer
	if viewerID := s.findViewerID(conn); viewerID != "" {
		s.room.RemoveViewer(viewerID)
		s.mu.Lock()
		delete(s.connToViewer, conn)
		s.mu.Unlock()
	}
}

func (s *Server) findViewerID(conn *websocket.Conn) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connToViewer[conn]
}

func sendMessage(conn *websocket.Conn, msg Message) {
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Failed to send message: %v", err)
	}
}
