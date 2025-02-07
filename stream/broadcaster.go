// stream/broadcaster.go
package stream

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"toktok/utils"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/intervalpli"
	"github.com/pion/webrtc/v3"
)

type Broadcaster struct {
	config              *webrtc.Configuration
	mediaEngine         *webrtc.MediaEngine
	interceptorRegistry *interceptor.Registry
	peerConnection      *webrtc.PeerConnection
	localTrack          *webrtc.TrackLocalStaticRTP
	mu                  sync.Mutex
	trackReady          chan struct{}
}

func NewBroadcaster(stunURL string, pliInterval int) (*Broadcaster, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("failed to register codecs: %w", err)
	}

	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return nil, fmt.Errorf("failed to register default interceptors: %w", err)
	}

	intervalPliFactory, err := intervalpli.NewReceiverInterceptor()
	if err != nil {
		return nil, fmt.Errorf("failed to create PLI interceptor: %w", err)
	}
	i.Add(intervalPliFactory)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{stunURL},
			},
		},
	}

	return &Broadcaster{
		config:              &config,
		mediaEngine:         m,
		interceptorRegistry: i,
		trackReady:          make(chan struct{}),
	}, nil
}

func (b *Broadcaster) Start(offer webrtc.SessionDescription) (string, error) {
	var err error
	log.Println("Creating new peer connection")
	b.peerConnection, err = webrtc.NewAPI(
		webrtc.WithMediaEngine(b.mediaEngine),
		webrtc.WithInterceptorRegistry(b.interceptorRegistry),
	).NewPeerConnection(*b.config)
	if err != nil {
		return "", fmt.Errorf("failed to create peer connection: %w", err)
	}

	log.Println("Adding video transceiver")
	if _, err = b.peerConnection.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo); err != nil {
		return "", fmt.Errorf("failed to add transceiver: %w", err)
	}

	localTrackChan := make(chan *webrtc.TrackLocalStaticRTP, 1)

	log.Println("Setting up OnTrack handler")
	b.peerConnection.OnTrack(func(remoteTrack *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Println("OnTrack triggered")
		localTrack, err := webrtc.NewTrackLocalStaticRTP(remoteTrack.Codec().RTPCodecCapability, "video", "pion")
		if err != nil {
			log.Printf("failed to create local track: %v\n", err)
			return
		}

		b.mu.Lock()
		b.localTrack = localTrack
		b.mu.Unlock()

		localTrackChan <- localTrack
		close(b.trackReady)

		log.Println("Starting RTP forwarding")
		rtpBuf := make([]byte, 1400)
		for {
			i, _, readErr := remoteTrack.Read(rtpBuf)
			if readErr != nil {
				log.Printf("failed to read from remote track: %v\n", readErr)
				return
			}

			if _, err = localTrack.Write(rtpBuf[:i]); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				log.Printf("failed to write to local track: %v\n", err)
				return
			}
		}
	})

	log.Println("Setting remote description")
	if err = b.peerConnection.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("failed to set remote description: %w", err)
	}

	log.Println("Creating answer")
	answer, err := b.peerConnection.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("failed to create answer: %w", err)
	}

	log.Println("Setting up ICE gathering")
	gatherComplete := webrtc.GatheringCompletePromise(b.peerConnection)

	log.Println("Setting local description")
	if err = b.peerConnection.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("failed to set local description: %w", err)
	}

	log.Println("Waiting for ICE gathering to complete")
	<-gatherComplete

	log.Println("ICE gathering completed, returning answer")
	return utils.Encode(*b.peerConnection.LocalDescription()), nil
}

func (b *Broadcaster) GetLocalTrack() (*webrtc.TrackLocalStaticRTP, error) {
	select {
	case <-b.trackReady:
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.localTrack == nil {
			return nil, fmt.Errorf("local track not initialized")
		}
		return b.localTrack, nil
	}
}

func (b *Broadcaster) Close() error {
	if b.peerConnection != nil {
		return b.peerConnection.Close()
	}
	return nil
}
