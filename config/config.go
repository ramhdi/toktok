// config/config.go
package config

import "flag"

type Config struct {
	Port          int
	StunURL       string
	MaxBufferSize int
	PLIInterval   int // in seconds
}

func NewConfig() *Config {
	port := flag.Int("port", 8080, "HTTP server port")
	flag.Parse()

	return &Config{
		Port:          *port,
		StunURL:       "stun:stun.l.google.com:19302",
		MaxBufferSize: 1400,
		PLIInterval:   3,
	}
}
