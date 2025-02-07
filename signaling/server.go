// signaling/server.go
package signaling

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
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

	// Server state
	broadcaster  *websocket.Conn
	viewers      map[*websocket.Conn]bool
	viewersMutex sync.RWMutex

	// Channels for SDP handling
	broadcasterSDPChan chan string
	viewerSDPChan      chan string

	// Server configuration
	port int
}

func NewServer(port int) *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for testing
			},
		},
		viewers:            make(map[*websocket.Conn]bool),
		broadcasterSDPChan: make(chan string),
		viewerSDPChan:      make(chan string),
		port:               port,
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
	if s.broadcaster != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Broadcaster already exists",
		})
		return
	}

	s.broadcaster = conn
	log.Println("Broadcaster registered")
}

func (s *Server) handleBroadcasterOffer(conn *websocket.Conn, sdp string) {
	if conn != s.broadcaster {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Not authorized as broadcaster",
		})
		return
	}

	s.broadcasterSDPChan <- sdp
	answer := <-s.broadcasterSDPChan // Wait for answer from main process

	sendMessage(conn, Message{
		Type: ServerBroadcasterAnswer,
		SDP:  answer,
	})
}

func (s *Server) handleViewerRegister(conn *websocket.Conn) {
	if s.broadcaster == nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "No active broadcaster",
		})
		return
	}

	s.viewersMutex.Lock()
	s.viewers[conn] = true
	s.viewersMutex.Unlock()
	log.Println("New viewer registered")
}

func (s *Server) handleViewerOffer(conn *websocket.Conn, sdp string) {
	s.viewersMutex.RLock()
	_, exists := s.viewers[conn]
	s.viewersMutex.RUnlock()

	if !exists {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Not registered as viewer",
		})
		return
	}

	s.viewerSDPChan <- sdp
	answer := <-s.viewerSDPChan // Wait for answer from main process

	sendMessage(conn, Message{
		Type: ServerViewerAnswer,
		SDP:  answer,
	})
}

func (s *Server) handleDisconnect(conn *websocket.Conn) {
	if conn == s.broadcaster {
		s.broadcaster = nil
		log.Println("Broadcaster disconnected")
	} else {
		s.viewersMutex.Lock()
		delete(s.viewers, conn)
		s.viewersMutex.Unlock()
		log.Println("Viewer disconnected")
	}
}

func (s *Server) GetBroadcasterSDPChan() chan string {
	return s.broadcasterSDPChan
}

func (s *Server) GetViewerSDPChan() chan string {
	return s.viewerSDPChan
}

func sendMessage(conn *websocket.Conn, msg Message) {
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Failed to send message: %v", err)
	}
}
