// stream/viewer.go
package stream

import (
	"fmt"
	"log"
	"toktok/utils"

	"github.com/pion/webrtc/v3"
)

type Viewer struct {
	config         *webrtc.Configuration
	peerConnection *webrtc.PeerConnection
}

func NewViewer(stunURL string) *Viewer {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{stunURL},
			},
		},
	}

	return &Viewer{
		config: &config,
	}
}

func (v *Viewer) Start(offer webrtc.SessionDescription, localTrack *webrtc.TrackLocalStaticRTP) (string, error) {
	var err error
	log.Println("Creating new peer connection")
	v.peerConnection, err = webrtc.NewPeerConnection(*v.config)
	if err != nil {
		return "", fmt.Errorf("failed to create peer connection: %w", err)
	}

	log.Println("Adding track")
	rtpSender, err := v.peerConnection.AddTrack(localTrack)
	if err != nil {
		return "", fmt.Errorf("failed to add track: %w", err)
	}

	go v.processRTCP(rtpSender)

	log.Println("Setting remote description")
	if err = v.peerConnection.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("failed to set remote description: %w", err)
	}

	log.Println("Creating answer")
	answer, err := v.peerConnection.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("failed to create answer: %w", err)
	}

	log.Println("Gathering complete promise")
	gatherComplete := webrtc.GatheringCompletePromise(v.peerConnection)

	log.Println("Setting local description")
	if err = v.peerConnection.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("failed to set local description: %w", err)
	}

	log.Println("Waiting for ICE gathering to complete")
	<-gatherComplete

	log.Println("ICE gathering completed, returning answer")
	return utils.Encode(*v.peerConnection.LocalDescription()), nil
}

func (v *Viewer) processRTCP(sender *webrtc.RTPSender) {
	rtcpBuf := make([]byte, 1500)
	for {
		if _, _, rtcpErr := sender.Read(rtcpBuf); rtcpErr != nil {
			return
		}
	}
}

func (v *Viewer) Close() error {
	if v.peerConnection != nil {
		return v.peerConnection.Close()
	}
	return nil
}
