package main

import (
	"sync"
	"time"
)

type entry struct {
	value  string
	list   []string
	stream []string
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
