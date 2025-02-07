// signaling/server.go
package signaling

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type Server struct {
	sdpChan chan string
	port    int
}

func NewServer(port int) *Server {
	return &Server{
		sdpChan: make(chan string),
		port:    port,
	}
}

func (s *Server) Start() error {
	http.HandleFunc("/sdp", s.handleSDP)
	return http.ListenAndServe(":"+strconv.Itoa(s.port), nil)
}

func (s *Server) handleSDP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "done")
	s.sdpChan <- string(body)
}

func (s *Server) GetSDPChan() chan string {
	return s.sdpChan
}
