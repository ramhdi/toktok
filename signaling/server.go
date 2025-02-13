// signaling/server.go
package signaling

import (
	"fmt"
	"log"
	"net/http"

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
	upgrader websocket.Upgrader
	rooms    *RoomManager
	port     int
}

func NewServer(port int) *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		rooms: NewRoomManager(),
		port:  port,
	}
}

func (s *Server) Start() error {
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/", fs)
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
	room, err := s.rooms.GetDefaultRoom()
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Failed to get default room",
		})
		return
	}

	if err := room.SetBroadcaster(conn); err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Broadcaster already exists",
		})
		return
	}

	log.Println("Broadcaster registered in default room")
}

func (s *Server) handleBroadcasterOffer(conn *websocket.Conn, sdp string) {
	room, err := s.rooms.GetDefaultRoom()
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Failed to get default room",
		})
		return
	}

	room.BroadcasterChan <- sdp
	answer := <-room.BroadcasterChan

	sendMessage(conn, Message{
		Type: ServerBroadcasterAnswer,
		SDP:  answer,
	})
}

func (s *Server) handleViewerRegister(conn *websocket.Conn) {
	room, err := s.rooms.GetDefaultRoom()
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Failed to get default room",
		})
		return
	}

	if err := room.AddViewer(conn); err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  err.Error(),
		})
		return
	}

	log.Println("New viewer registered in default room")
}

func (s *Server) handleViewerOffer(conn *websocket.Conn, sdp string) {
	room, err := s.rooms.GetDefaultRoom()
	if err != nil {
		sendMessage(conn, Message{
			Type: "error",
			SDP:  "Failed to get default room",
		})
		return
	}

	room.ViewerChan <- sdp
	answer := <-room.ViewerChan

	sendMessage(conn, Message{
		Type: ServerViewerAnswer,
		SDP:  answer,
	})
}

func (s *Server) handleDisconnect(conn *websocket.Conn) {
	room, err := s.rooms.GetDefaultRoom()
	if err != nil {
		return
	}

	room.RemoveViewer(conn)
	s.rooms.DeleteRoom("default") // This will handle broadcaster disconnection
}

func (s *Server) GetBroadcasterSDPChan() chan string {
	room, _ := s.rooms.GetDefaultRoom()
	return room.BroadcasterChan
}

func (s *Server) GetViewerSDPChan() chan string {
	room, _ := s.rooms.GetDefaultRoom()
	return room.ViewerChan
}

func sendMessage(conn *websocket.Conn, msg Message) {
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Failed to send message: %v", err)
	}
}
