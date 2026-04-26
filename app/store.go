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
var mu sync.Mutex

func deleteAfter(key string, delay time.Duration) {
	time.Sleep(delay)
	mu.Lock()
	delete(store, key)
	mu.Unlock()
}
