package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var waiters = map[string][]chan string{}

func parseID(id string) (int64, int64) {
	p := strings.Split(id, "-")
	if len(p) != 2 {
		return 0, 0
	}
	t, _ := strconv.ParseInt(p[0], 10, 64)
	s, _ := strconv.ParseInt(p[1], 10, 64)
	return t, s
}

func isGreaterID(newID, lastID string) bool {
	nt, ns := parseID(newID)
	lt, ls := parseID(lastID)
	if nt != lt {
		return nt > lt
	}
	return ns > ls
}

type client struct {
	conn    net.Conn
	inMulti bool
	queue   [][]string
	watched map[string]uint64
}

// watchedKeysModified reports whether any key watched by this client has been
// modified since the WATCH call. The caller must hold mu.
func (c *client) watchedKeysModified() bool {
	for key, ver := range c.watched {
		if versions[key] != ver {
			return true
		}
	}
	return false
}

func (c *client) handle(parts []string) bool {
	command := strings.ToLower(parts[0])
	switch command {
	case "multi":
		if c.inMulti {
			c.conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
			return false
		}
		c.inMulti = true
		c.queue = nil
		c.conn.Write([]byte("+OK\r\n"))
		return false
	case "exec":
		if !c.inMulti {
			c.conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			return false
		}
		queued := c.queue
		c.inMulti = false
		c.queue = nil

		mu.Lock()
		aborted := c.watchedKeysModified()
		mu.Unlock()
		c.watched = nil
		if aborted {
			c.conn.Write([]byte("*-1\r\n"))
			return false
		}

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "*%d\r\n", len(queued))
		for _, q := range queued {
			dispatch(&buf, q, false)
		}
		c.conn.Write(buf.Bytes())
		return false
	case "discard":
		if !c.inMulti {
			c.conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			return false
		}
		c.inMulti = false
		c.queue = nil
		c.watched = nil
		c.conn.Write([]byte("+OK\r\n"))
		return false
	case "watch":
		if c.inMulti {
			c.conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
			return false
		}
		mu.Lock()
		if c.watched == nil {
			c.watched = make(map[string]uint64)
		}
		for _, key := range parts[1:] {
			c.watched[key] = versions[key]
		}
		mu.Unlock()
		c.conn.Write([]byte("+OK\r\n"))
		return false
	case "unwatch":
		c.watched = nil
		c.conn.Write([]byte("+OK\r\n"))
		return false
	}

	if c.inMulti {
		c.queue = append(c.queue, parts)
		c.conn.Write([]byte("+QUEUED\r\n"))
		return false
	}

	return dispatch(c.conn, parts, true)
}

type streamRead struct {
	key     string
	entries []streamEntry
}

// readStreams collects entries whose IDs are greater than the given IDs.
// The caller must hold mu.
func readStreams(keys, ids []string) []streamRead {
	var results []streamRead
	for i, key := range keys {
		item := store[key]
		var matched []streamEntry
		for _, e := range item.stream {
			if isGreaterID(e.id, ids[i]) {
				matched = append(matched, e)
			}
		}
		if len(matched) > 0 {
			results = append(results, streamRead{key: key, entries: matched})
		}
	}
	return results
}

func writeStreams(w io.Writer, results []streamRead) {
	if len(results) == 0 {
		w.Write([]byte("*-1\r\n"))
		return
	}

	fmt.Fprintf(w, "*%d\r\n", len(results))
	for _, r := range results {
		fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(r.key), r.key, len(r.entries))
		for _, e := range r.entries {
			fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(e.id), e.id, len(e.fields))
			for _, f := range e.fields {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(f), f)
			}
		}
	}
}

func dispatch(w io.Writer, parts []string, allowBlocking bool) bool {
	command := strings.ToLower(parts[0])
	switch command {
	case "ping":
		w.Write([]byte("+PONG\r\n"))
	case "echo":
		arg := parts[1]
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg)
	case "set":
		mu.Lock()
		store[parts[1]] = entry{value: parts[2]}
		touch(parts[1])
		mu.Unlock()
		if len(parts) > 3 && strings.ToLower(parts[3]) == "px" {
			ms, _ := strconv.Atoi(parts[4])
			go deleteAfter(parts[1], time.Duration(ms)*time.Millisecond)
		}
		w.Write([]byte("+OK\r\n"))
	case "get":
		mu.Lock()
		val, ok := store[parts[1]]
		mu.Unlock()
		if ok {
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val.value), val.value)
		} else {
			w.Write([]byte("$-1\r\n"))
		}
	case "incr":
		mu.Lock()
		item, ok := store[parts[1]]
		if !ok {
			store[parts[1]] = entry{value: "1"}
			touch(parts[1])
			mu.Unlock()
			w.Write([]byte(":1\r\n"))
			return false
		}
		n, err := strconv.ParseInt(item.value, 10, 64)
		if err != nil {
			mu.Unlock()
			w.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return false
		}
		n++
		item.value = strconv.FormatInt(n, 10)
		store[parts[1]] = item
		touch(parts[1])
		mu.Unlock()
		fmt.Fprintf(w, ":%d\r\n", n)
	case "rpush":
		mu.Lock()
		item := store[parts[1]]
		for _, val := range parts[2:] {
			item.list = append(item.list, val)
		}
		store[parts[1]] = item
		listLen := len(item.list)
		touch(parts[1])

		for len(waiters[parts[1]]) > 0 && len(item.list) > 0 {
			ch := waiters[parts[1]][0]
			waiters[parts[1]] = waiters[parts[1]][1:]
			val := item.list[0]
			item.list = item.list[1:]
			store[parts[1]] = item
			ch <- val
		}
		mu.Unlock()
		fmt.Fprintf(w, ":%d\r\n", listLen)
	case "lrange":
		mu.Lock()
		item := store[parts[1]]
		mu.Unlock()

		start, _ := strconv.Atoi(parts[2])
		end, _ := strconv.Atoi(parts[3])

		if start < 0 {
			start = max(0, len(item.list)+start)
		}
		if end < 0 {
			end = len(item.list) + end
		}

		if start >= len(item.list) || start > end {
			w.Write([]byte("*0\r\n"))
			return false
		}
		if end >= len(item.list) {
			end = len(item.list) - 1
		}

		sublist := item.list[start : end+1]
		fmt.Fprintf(w, "*%d\r\n", len(sublist))
		for _, val := range sublist {
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
		}
	case "lpush":
		mu.Lock()
		item := store[parts[1]]
		for _, val := range parts[2:] {
			item.list = append([]string{val}, item.list...)
		}
		store[parts[1]] = item
		touch(parts[1])
		mu.Unlock()
		fmt.Fprintf(w, ":%d\r\n", len(item.list))
	case "llen":
		mu.Lock()
		item := store[parts[1]]
		mu.Unlock()
		fmt.Fprintf(w, ":%d\r\n", len(item.list))
	case "lpop":
		mu.Lock()
		item := store[parts[1]]

		if len(parts) > 2 {
			count, _ := strconv.Atoi(parts[2])
			if count > len(item.list) {
				count = len(item.list)
			}
			popped := item.list[:count]
			item.list = item.list[count:]
			store[parts[1]] = item
			touch(parts[1])
			mu.Unlock()
			fmt.Fprintf(w, "*%d\r\n", len(popped))
			for _, val := range popped {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
			}
		} else {
			val := item.list[0]
			item.list = item.list[1:]
			store[parts[1]] = item
			touch(parts[1])
			mu.Unlock()
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
		}
	case "blpop":
		key := parts[1]
		timeoutSec, _ := strconv.ParseFloat(parts[2], 64)

		mu.Lock()
		e := store[key]
		if len(e.list) > 0 {
			val := e.list[0]
			e.list = e.list[1:]
			store[key] = e
			touch(key)
			mu.Unlock()
			fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			return false
		}
		mu.Unlock()

		if !allowBlocking {
			w.Write([]byte("*-1\r\n"))
			return false
		}

		ch := make(chan string, 1)
		mu.Lock()
		waiters[key] = append(waiters[key], ch)
		mu.Unlock()

		if timeoutSec == 0 {
			val := <-ch
			fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		} else {
			timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
			select {
			case val := <-ch:
				timer.Stop()
				fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			case <-timer.C:
				mu.Lock()
				for i, waiter := range waiters[key] {
					if waiter == ch {
						waiters[key] = append(waiters[key][:i], waiters[key][i+1:]...)
						break
					}
				}
				mu.Unlock()
				w.Write([]byte("*-1\r\n"))
			}
		}
	case "type":
		mu.Lock()
		e, ok := store[parts[1]]
		mu.Unlock()

		if !ok {
			w.Write([]byte("+none\r\n"))
			return false
		}

		if len(e.list) > 0 {
			w.Write([]byte("+list\r\n"))
		} else if len(e.stream) > 0 {
			w.Write([]byte("+stream\r\n"))
		} else {
			w.Write([]byte("+string\r\n"))
		}
	case "xadd":
		key := parts[1]
		id := parts[2]

		mu.Lock()
		item := store[key]

		if id == "*" {
			nowMs := time.Now().UnixMilli()
			var seq int64 = 0
			if len(item.stream) > 0 {
				lt, ls := parseID(item.stream[len(item.stream)-1].id)
				if lt == nowMs {
					seq = ls + 1
				}
			}
			id = fmt.Sprintf("%d-%d", nowMs, seq)
		} else if strings.HasSuffix(id, "-*") {
			timePart := strings.TrimSuffix(id, "-*")
			timeInt, _ := strconv.ParseInt(timePart, 10, 64)
			var seq int64 = 0
			if timeInt == 0 {
				seq = 1
			}
			for i := len(item.stream) - 1; i >= 0; i-- {
				t, s := parseID(item.stream[i].id)
				if t == timeInt {
					seq = s + 1
					break
				} else if t < timeInt {
					break
				}
			}
			id = fmt.Sprintf("%s-%d", timePart, seq)
		}

		if id == "0-0" {
			mu.Unlock()
			w.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
			return false
		}
		if len(item.stream) > 0 {
			last := item.stream[len(item.stream)-1].id
			if !isGreaterID(id, last) {
				mu.Unlock()
				w.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
				return false
			}
		} else if !isGreaterID(id, "0-0") {
			mu.Unlock()
			w.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
			return false
		}

		item.stream = append(item.stream, streamEntry{id: id, fields: parts[3:]})
		store[key] = item
		touch(key)
		mu.Unlock()

		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(id), id)
	case "xread":
		blockMs := int64(-1)
		idx := 1
		if idx < len(parts) && strings.ToLower(parts[idx]) == "block" {
			if idx+1 >= len(parts) {
				w.Write([]byte("-ERR syntax error\r\n"))
				return false
			}
			ms, err := strconv.ParseInt(parts[idx+1], 10, 64)
			if err != nil || ms < 0 {
				w.Write([]byte("-ERR timeout is not an integer or out of range\r\n"))
				return false
			}
			blockMs = ms
			idx += 2
		}
		if idx >= len(parts) || strings.ToLower(parts[idx]) != "streams" {
			w.Write([]byte("-ERR syntax error\r\n"))
			return false
		}

		rest := parts[idx+1:]
		if len(rest) == 0 || len(rest)%2 != 0 {
			w.Write([]byte("-ERR syntax error\r\n"))
			return false
		}
		n := len(rest) / 2
		keys := rest[:n]
		ids := rest[n:]

		mu.Lock()
		resolved := make([]string, n)
		for i, key := range keys {
			id := ids[i]
			if id == "$" {
				item := store[key]
				if len(item.stream) > 0 {
					id = item.stream[len(item.stream)-1].id
				} else {
					id = "0-0"
				}
			} else if !strings.Contains(id, "-") {
				id = id + "-0"
			}
			resolved[i] = id
		}
		results := readStreams(keys, resolved)
		mu.Unlock()

		if len(results) == 0 && blockMs >= 0 && allowBlocking {
			var deadline time.Time
			if blockMs > 0 {
				deadline = time.Now().Add(time.Duration(blockMs) * time.Millisecond)
			}
			for {
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
				results = readStreams(keys, resolved)
				mu.Unlock()
				if len(results) > 0 {
					break
				}
				if blockMs > 0 && time.Now().After(deadline) {
					break
				}
			}
		}

		writeStreams(w, results)
	case "xrange":
		key := parts[1]
		start := parts[2]
		end := parts[3]

		if !strings.Contains(start, "-") {
			start = start + "-0"
		}
		if !strings.Contains(end, "-") {
			end = end + "-9223372036854775807"
		}

		mu.Lock()
		item := store[key]
		mu.Unlock()

		var matched []streamEntry
		for _, e := range item.stream {
			if !isGreaterID(start, e.id) && !isGreaterID(e.id, end) {
				matched = append(matched, e)
			}
		}

		fmt.Fprintf(w, "*%d\r\n", len(matched))
		for _, e := range matched {
			fmt.Fprintf(w, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(e.id), e.id, len(e.fields))
			for _, f := range e.fields {
				fmt.Fprintf(w, "$%d\r\n%s\r\n", len(f), f)
			}
		}
	}
	return false
}
