package main

import (
	"fmt"
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

// handleCommand dispatches the parsed command to the appropriate handler.
// Returns true if the connection should be closed after this command.
func handleCommand(conn net.Conn, parts []string) bool {
	command := strings.ToLower(parts[0])
	switch command {
	case "ping":
		conn.Write([]byte("+PONG\r\n"))
	case "echo":
		arg := parts[1]
		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(arg), arg)
	case "set":
		mu.Lock()
		store[parts[1]] = entry{value: parts[2]}
		mu.Unlock()
		if len(parts) > 3 && strings.ToLower(parts[3]) == "px" {
			ms, _ := strconv.Atoi(parts[4])
			go deleteAfter(parts[1], time.Duration(ms)*time.Millisecond)
		}
		conn.Write([]byte("+OK\r\n"))
	case "get":
		mu.Lock()
		val, ok := store[parts[1]]
		mu.Unlock()
		if ok {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val.value), val.value)
		} else {
			conn.Write([]byte("$-1\r\n"))
		}
	case "rpush":
		mu.Lock()
		item := store[parts[1]]
		for _, val := range parts[2:] {
			item.list = append(item.list, val)
		}
		store[parts[1]] = item
		listLen := len(item.list)

		for len(waiters[parts[1]]) > 0 && len(item.list) > 0 {
			ch := waiters[parts[1]][0]
			waiters[parts[1]] = waiters[parts[1]][1:]
			val := item.list[0]
			item.list = item.list[1:]
			store[parts[1]] = item
			ch <- val
		}
		mu.Unlock()
		fmt.Fprintf(conn, ":%d\r\n", listLen)
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
			conn.Write([]byte("*0\r\n"))
			return false
		}
		if end >= len(item.list) {
			end = len(item.list) - 1
		}

		sublist := item.list[start : end+1]
		fmt.Fprintf(conn, "*%d\r\n", len(sublist))
		for _, val := range sublist {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
		}
	case "lpush":
		mu.Lock()
		item := store[parts[1]]
		for _, val := range parts[2:] {
			item.list = append([]string{val}, item.list...)
		}
		store[parts[1]] = item
		mu.Unlock()
		fmt.Fprintf(conn, ":%d\r\n", len(item.list))
	case "llen":
		mu.Lock()
		item := store[parts[1]]
		mu.Unlock()
		fmt.Fprintf(conn, ":%d\r\n", len(item.list))
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
			mu.Unlock()
			fmt.Fprintf(conn, "*%d\r\n", len(popped))
			for _, val := range popped {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
			}
		} else {
			val := item.list[0]
			item.list = item.list[1:]
			store[parts[1]] = item
			mu.Unlock()
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
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
			mu.Unlock()
			fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			return false
		}
		mu.Unlock()

		ch := make(chan string, 1)
		mu.Lock()
		waiters[key] = append(waiters[key], ch)
		mu.Unlock()

		if timeoutSec == 0 {
			val := <-ch
			fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		} else {
			timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
			select {
			case val := <-ch:
				timer.Stop()
				fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
			case <-timer.C:
				mu.Lock()
				for i, w := range waiters[key] {
					if w == ch {
						waiters[key] = append(waiters[key][:i], waiters[key][i+1:]...)
						break
					}
				}
				mu.Unlock()
				conn.Write([]byte("*-1\r\n"))
			}
		}
	case "type":
		mu.Lock()
		e, ok := store[parts[1]]
		mu.Unlock()

		if !ok {
			conn.Write([]byte("+none\r\n"))
			return false
		}

		if len(e.list) > 0 {
			conn.Write([]byte("+list\r\n"))
		} else if len(e.stream) > 0 {
			conn.Write([]byte("+stream\r\n"))
		} else {
			conn.Write([]byte("+string\r\n"))
		}
	case "xadd":
		key := parts[1]
		id := parts[2]

		mu.Lock()
		item := store[key]

		// Auto-generate sequence number for "<time>-*" format
		if strings.HasSuffix(id, "-*") {
			timePart := strings.TrimSuffix(id, "-*")
			timeInt, _ := strconv.ParseInt(timePart, 10, 64)
			var seq int64 = 0
			if timeInt == 0 {
				seq = 1
			}
			// Find the last entry with matching time part to determine next seq
			for i := len(item.stream) - 1; i >= 0; i-- {
				t, s := parseID(item.stream[i])
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
			conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
			return false
		}
		if len(item.stream) > 0 {
			last := item.stream[len(item.stream)-1]
			if !isGreaterID(id, last) {
				mu.Unlock()
				conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
				return false
			}
		} else if !isGreaterID(id, "0-0") {
			mu.Unlock()
			conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
			return false
		}

		item.stream = append(item.stream, id)
		store[key] = item
		mu.Unlock()

		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
	}
	return false
}
