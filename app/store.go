package main

import (
	"sync"
	"time"
)

type streamEntry struct {
	id     string
	fields []string // alternating key-value pairs
}

type entry struct {
	value  string
	list   []string
	stream []streamEntry
	expiry time.Time
}

var store = map[string]entry{}
var versions = map[string]uint64{}
var mu sync.Mutex

// touch records that a key was modified. The caller must hold mu.
func touch(key string) {
	versions[key]++
}

func deleteAfter(key string, delay time.Duration) {
	time.Sleep(delay)
	mu.Lock()
	delete(store, key)
	touch(key)
	mu.Unlock()
}
