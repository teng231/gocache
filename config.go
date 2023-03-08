package gocache

import (
	"log"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type Config struct {
	Shard        int
	OnRemove     func([]byte, *Item)
	TTL          time.Duration
	CleanWindow  time.Duration
	RefreshShard time.Duration
	IsManualGC   bool
	Verbose      bool
}

func DefaultConfig() *Config {
	return &Config{
		Shard:        256,
		OnRemove:     nil,
		CleanWindow:  5 * time.Minute,
		TTL:          12 * time.Hour,
		RefreshShard: 12*time.Hour + time.Minute,
		IsManualGC:   true,
		Verbose:      true,
	}
}
