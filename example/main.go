package main

import (
	"log"

	"github.com/teng231/gocache"
)

func main() {
	engine, err := gocache.New(gocache.DefaultConfig())
	if err != nil {
		panic(err)
	}

	engine.Set([]byte("key1"), []byte("value1"))
	engine.Set([]byte("key2"), []byte("value2"))
	engine.Set([]byte("key3"), []byte("value3"))

	log.Print(engine.Len(), " ")

	data, err := engine.Get([]byte("key1"))
	log.Print(data, err)
}
