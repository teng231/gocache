package gocache

import (
	"log"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type Config struct {
	OnRemove    func(string, *Item)
	TTL         time.Duration
	CleanWindow time.Duration
	Verbose     bool
}

func DefaultConfig() *Config {
	return &Config{
		OnRemove:    nil,
		CleanWindow: 5 * time.Minute,
		TTL:         12 * time.Hour,
		Verbose:     true,
	}
}
