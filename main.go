package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/h264reader"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type ServerConfig struct {
	Port        int    `yaml:"port"`
	FrontendDir string `yaml:"frontend_dir"`
}

type CameraConfig struct {
	Device      string `yaml:"device"`
	InputFormat string `yaml:"input_format"`
	Width       int    `yaml:"width"`
	Height      int    `yaml:"height"`
}

type EncodingConfig struct {
	Codec        string `yaml:"codec"` // "h264" or "vp8"
	FPS          int    `yaml:"fps"`
	Bitrate      string `yaml:"bitrate"`
	MaxBitrate   string `yaml:"max_bitrate"`
	BufferSize   string `yaml:"buffer_size"`
	H264Preset   string `yaml:"h264_preset"`
	H264Tune     string `yaml:"h264_tune"`
	H264Profile  string `yaml:"h264_profile"`
}

type WebRTCConfig struct {
	STUNServers []string `yaml:"stun_servers"`
}

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Camera   CameraConfig   `yaml:"camera"`
	Encoding EncodingConfig `yaml:"encoding"`
	WebRTC   WebRTCConfig   `yaml:"webrtc"`
}

func defaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Port:        8080,
			FrontendDir: "./frontend",
		},
		Camera: CameraConfig{
			Device:      "/dev/video0",
			InputFormat: "mjpeg",
			Width:       640,
			Height:      480,
		},
		Encoding: EncodingConfig{
			Codec:       "h264",
			FPS:         30,
			Bitrate:     "2M",
			MaxBitrate:  "2M",
			BufferSize:  "4M",
			H264Preset:  "ultrafast",
			H264Tune:    "zerolatency",
			H264Profile: "baseline",
		},
		WebRTC: WebRTCConfig{
			STUNServers: []string{},
		},
	}
}

func loadConfig(path string) (Config, error) {
	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No config file at %s — using defaults", path)
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var cfg Config

// ---------------------------------------------------------------------------
// Signaling message exchanged over WebSocket
// ---------------------------------------------------------------------------

type signalMessage struct {
	Type      string                     `json:"type"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	var err error
	cfg, err = loadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Config loaded: codec=%s fps=%d device=%s port=%d",
		cfg.Encoding.Codec, cfg.Encoding.FPS, cfg.Camera.Device, cfg.Server.Port)

	http.Handle("/", http.FileServer(http.Dir(cfg.Server.FrontendDir)))
	http.HandleFunc("/signal", handleSignal)

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	log.Printf("Server listening on %s (client at http://<this-ip>%s/)", addr, addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// ---------------------------------------------------------------------------
// WebSocket → WebRTC signaling handler
// ---------------------------------------------------------------------------

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

	// Build WebRTC configuration from config
	webrtcConfig := webrtc.Configuration{}
	if len(cfg.WebRTC.STUNServers) > 0 {
		webrtcConfig.ICEServers = []webrtc.ICEServer{
			{URLs: cfg.WebRTC.STUNServers},
		}
	}

	peerConnection, err := webrtc.NewPeerConnection(webrtcConfig)
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

	// Choose MIME type based on configured codec
	var codecCap webrtc.RTPCodecCapability
	switch cfg.Encoding.Codec {
	case "h264":
		codecCap = webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264,
			// These parameters are REQUIRED for browser SDP negotiation:
			// - profile-level-id: 42e01f = Constrained Baseline, Level 3.1
			// - packetization-mode: 1 = non-interleaved (standard WebRTC)
			// - level-asymmetry-allowed: 1 = standard for WebRTC
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		}
	case "vp8":
		codecCap = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}
	default:
		log.Printf("unsupported codec %q — falling back to h264", cfg.Encoding.Codec)
		codecCap = webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		}
	}

	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		codecCap,
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

// ---------------------------------------------------------------------------
// ffmpeg capture + encode → WebRTC track
// ---------------------------------------------------------------------------

// streamWebcam shells out to ffmpeg to capture the configured camera device,
// encodes to the configured codec, reads frames, and writes them as Pion
// media.Samples with proper pacing.
func streamWebcam(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	fps := cfg.Encoding.FPS
	fpsStr := strconv.Itoa(fps)
	resolution := fmt.Sprintf("%dx%d", cfg.Camera.Width, cfg.Camera.Height)
	frameDuration := time.Second / time.Duration(fps)

	var args []string

	// Common input args
	args = append(args,
		"-f", "v4l2",
		"-input_format", cfg.Camera.InputFormat,
		"-framerate", fpsStr,
		"-video_size", resolution,
		"-i", cfg.Camera.Device,
	)

	switch cfg.Encoding.Codec {
	case "h264":
		args = append(args,
			"-c:v", "libx264",
			"-preset", cfg.Encoding.H264Preset,
			"-tune", cfg.Encoding.H264Tune,
			"-profile:v", cfg.Encoding.H264Profile,
			"-b:v", cfg.Encoding.Bitrate,
			"-maxrate", cfg.Encoding.MaxBitrate,
			"-bufsize", cfg.Encoding.BufferSize,
			"-g", fpsStr, // keyframe every 1 second
			"-bsf:v", "dump_extra", // repeat SPS/PPS before every keyframe
			"-f", "h264",
			"-",
		)
	case "vp8":
		args = append(args,
			"-c:v", "libvpx",
			"-b:v", cfg.Encoding.Bitrate,
			"-maxrate", cfg.Encoding.MaxBitrate,
			"-bufsize", cfg.Encoding.BufferSize,
			"-deadline", "realtime",
			"-cpu-used", "4",
			"-f", "ivf",
			"-",
		)
	default:
		log.Printf("unsupported codec %q in streamWebcam", cfg.Encoding.Codec)
		return
	}

	log.Printf("Starting ffmpeg: ffmpeg %v", args)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("ffmpeg stdout pipe error:", err)
		return
	}

	// Wire ffmpeg stderr to Go logger so encoding errors are visible
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Println("ffmpeg stderr pipe error:", err)
		return
	}
	go func() {
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 1024*64), 1024*64)
		for scanner.Scan() {
			log.Println("[ffmpeg]", scanner.Text())
		}
	}()

	if err := cmd.Start(); err != nil {
		log.Println("ffmpeg start error:", err)
		return
	}
	defer cmd.Wait()

	switch cfg.Encoding.Codec {
	case "h264":
		streamH264(ctx, stdout, track, fps, frameDuration)
	case "vp8":
		streamVP8(ctx, stdout, track, fps, frameDuration)
	}
}

// streamH264 reads H.264 NAL units from the ffmpeg output, aggregates them
// into complete access units, and sends each as a single WriteSample call.
//
// Why aggregation matters:
//   - Each WriteSample call sets the RTP marker bit on its last packet,
//     signaling "end of access unit — decode now."
//   - If SPS/PPS/slice are separate WriteSample calls, the decoder gets
//     "decode now" after just the SPS and fails.
//   - Aggregating into one call means the marker bit is only set after the
//     last fragment of the complete access unit (SPS+PPS+slice together).
//   - Annex-B start codes are prepended so Pion's payloader can split
//     the NALs internally for proper RTP packetization (single/STAP-A/FU-A).
func streamH264(ctx context.Context, r io.Reader, track *webrtc.TrackLocalStaticSample, fps int, frameDuration time.Duration) {
	h264, err := h264reader.NewReader(r)
	if err != nil {
		log.Println("h264reader error:", err)
		return
	}

	annexBPrefix := []byte{0x00, 0x00, 0x00, 0x01}

	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	frameCount := 0

	// accessUnit accumulates Annex-B formatted NALs until a complete frame
	// is ready to send.
	var accessUnit []byte

	for {
		nal, err := h264.NextNAL()
		if err == io.EOF {
			log.Println("H.264 stream ended")
			return
		}
		if err != nil {
			log.Println("H.264 read error:", err)
			return
		}

		isVCL := nal.UnitType == h264reader.NalUnitTypeCodedSliceIdr ||
			nal.UnitType == h264reader.NalUnitTypeCodedSliceNonIdr

		// Log NAL types for the first few frames to help debug
		if frameCount < 3 {
			log.Printf("[h264] NAL type=%d size=%d bytes isVCL=%v", nal.UnitType, len(nal.Data), isVCL)
		}

		// Append this NAL with Annex-B start code to the access unit buffer.
		accessUnit = append(accessUnit, annexBPrefix...)
		accessUnit = append(accessUnit, nal.Data...)

		if !isVCL {
			// Non-VCL NAL (SPS/PPS/SEI/AUD) — keep buffering.
			continue
		}

		// VCL NAL received — the access unit is complete. Pace and send
		// everything as a single WriteSample so the marker bit is only
		// set on the last RTP packet of the complete access unit.
		select {
		case <-ctx.Done():
			log.Println("streaming context cancelled")
			return
		case <-ticker.C:
		}

		if err := track.WriteSample(media.Sample{Data: accessUnit, Duration: frameDuration}); err != nil {
			log.Println("write sample error:", err)
			return
		}

		accessUnit = nil

		frameCount++
		if frameCount%90 == 0 {
			log.Printf("Sent %d H.264 frames to client...", frameCount)
		}
	}
}

// streamVP8 reads IVF-framed VP8 packets from the ffmpeg output and writes
// them to the WebRTC track with ticker-based pacing.
func streamVP8(ctx context.Context, r io.Reader, track *webrtc.TrackLocalStaticSample, fps int, frameDuration time.Duration) {
	ivf, _, err := ivfreader.NewWith(r)
	if err != nil {
		log.Println("ivfreader error:", err)
		return
	}

	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	frameCount := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("streaming context cancelled")
			return
		case <-ticker.C:
		}

		frame, _, err := ivf.ParseNextFrame()
		if err == io.EOF {
			log.Println("VP8 stream ended")
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
			log.Printf("Sent %d VP8 frames to client...", frameCount)
		}
	}
}
