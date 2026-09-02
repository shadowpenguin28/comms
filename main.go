package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os/exec"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type signalMessage struct {
	Type      string                     `json:"type"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

func main() {
	// Serve the minimal client at "/" so your phone can just open http://<server-ip>:8080/
	http.Handle("/", http.FileServer(http.Dir("./frontend")))
	http.HandleFunc("/signal", handleSignal)

	log.Println("Server listening on :8080 (client at http://<this-ip>:8080/, signaling at /signal)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleSignal(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()
	log.Println("client connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := webrtc.Configuration{} // no STUN/TURN — same-LAN test for now
	peerConnection, err := webrtc.NewPeerConnection(config)
	if err != nil {
		log.Println("peer connection error:", err)
		return
	}
	defer peerConnection.Close()

	peerConnection.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		if err := conn.WriteJSON(signalMessage{Type: "candidate", Candidate: &init}); err != nil {
			log.Println("write candidate error:", err)
		}
	})

	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"video", "webcam",
	)
	if err != nil {
		log.Println("track error:", err)
		return
	}
	if _, err = peerConnection.AddTrack(videoTrack); err != nil {
		log.Println("add track error:", err)
		return
	}

	peerConnection.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		log.Println("ICE connection state:", s.String())
		if s == webrtc.ICEConnectionStateConnected {
			go streamWebcam(ctx, videoTrack)
		} else if s == webrtc.ICEConnectionStateDisconnected || s == webrtc.ICEConnectionStateFailed || s == webrtc.ICEConnectionStateClosed {
			cancel()
		}
	})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Println("read error (client likely disconnected):", err)
			return
		}

		var msg signalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Println("bad message:", err)
			continue
		}

		switch msg.Type {
		case "offer":
			if err := peerConnection.SetRemoteDescription(*msg.SDP); err != nil {
				log.Println("set remote description error:", err)
				continue
			}
			answer, err := peerConnection.CreateAnswer(nil)
			if err != nil {
				log.Println("create answer error:", err)
				continue
			}
			if err := peerConnection.SetLocalDescription(answer); err != nil {
				log.Println("set local description error:", err)
				continue
			}
			if err := conn.WriteJSON(signalMessage{Type: "answer", SDP: peerConnection.LocalDescription()}); err != nil {
				log.Println("write answer error:", err)
			}

		case "candidate":
			if msg.Candidate == nil {
				continue
			}
			if err := peerConnection.AddICECandidate(*msg.Candidate); err != nil {
				log.Println("add ice candidate error:", err)
			}

		default:
			log.Println("unhandled message type:", msg.Type)
		}
	}
}

// streamWebcam shells out to ffmpeg to capture + encode /dev/video4 as raw
// IVF VP8, then reads frames one at a time and writes each as a Pion media.Sample.
// This is much more robust than H.264 for WebRTC (avoids NAL timestamp/profile bugs).
func streamWebcam(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	const frameRate = 30

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-f", "v4l2",
		"-input_format", "mjpeg",
		"-framerate", "24",
		"-video_size", "640x480",
		"-i", "/dev/video4",
		"-c:v", "libvpx",
		"-b:v", "2M",
		"-maxrate", "2M",
		"-bufsize", "4M",
		"-deadline", "realtime",
		"-cpu-used", "4",
		"-f", "ivf",
		"-",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("ffmpeg stdout pipe error:", err)
		return
	}
	cmd.Stderr = nil // silence ffmpeg's own logging for now; re-enable if debugging capture issues

	if err := cmd.Start(); err != nil {
		log.Println("ffmpeg start error:", err)
		return
	}
	defer cmd.Wait()

	ivf, _, err := ivfreader.NewWith(stdout)
	if err != nil {
		log.Println("ivfreader error:", err)
		return
	}

	frameDuration := time.Second / frameRate
	frameCount := 0

	for {
		frame, _, err := ivf.ParseNextFrame()
		if err == io.EOF {
			log.Println("ffmpeg stream ended")
			return
		}
		if err != nil {
			log.Println("IVF read error:", err)
			return
		}

		if err := track.WriteSample(media.Sample{Data: frame, Duration: frameDuration}); err != nil {
			log.Println("write sample error:", err)
			return
		}

		frameCount++
		if frameCount%90 == 0 {
			log.Printf("Successfully sent %d VP8 frames to phone...", frameCount)
		}
	}
}
