package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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
var waiters = map[string][]chan string{}

func parseRESP(input string) []string {
	parts := strings.Split(input, "\r\n")
	var result []string
	for _, part := range parts {
		if part == "" || part[0] == '*' || part[0] == '$' {
			continue
		}
		result = append(result, part)
	}
	return result
}

func deleteAfter(key string, delay time.Duration) {
	time.Sleep(delay)
	mu.Lock()
	delete(store, key)
	mu.Unlock()
}

func isGreaterID(newID, lastID string) bool {
	nt, ns := parseID(newID)
	lt, ls := parseID(lastID)
	if nt != lt {
		return nt > lt
	}
	return ns > ls
}

func parseID(id string) (int64, int64) {
	p := strings.Split(id, "-")
	if len(p) != 2 {
		return 0, 0
	}
	t, _ := strconv.ParseInt(p[0], 10, 64)
	s, _ := strconv.ParseInt(p[1], 10, 64)
	return t, s
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading buffer: ", err.Error())
			return
		}

		parts := parseRESP(string(buf[:n]))
		if len(parts) == 0 {
			continue
		}

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

			// Notify waiters (BLPOP clients) in FIFO order
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
				continue
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
				return
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
					conn.Write([]byte("*-1\r\n"))
				}
			}
		case "type":
			mu.Lock()
			entry, ok := store[parts[1]]
			mu.Unlock()

			if !ok {
				conn.Write([]byte("+none\r\n"))
				return
			}

			if len(entry.list) > 0 {
				conn.Write([]byte("+list\r\n"))
			} else if len(entry.stream) > 0 {
				conn.Write([]byte("+stream\r\n"))
			} else {
				conn.Write([]byte("+string\r\n"))
			}
		case "xadd":
			key := parts[1]
			id := parts[2]

			mu.Lock()
			item := store[key]
			if len(item.stream) > 0 {
				last := item.stream[len(item.stream)-1]
				if !isGreaterID(id, last) {
					mu.Unlock()
					conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
					return
				}
			} else if id == "0-0" || !isGreaterID(id, "0-0") {
				mu.Unlock()
				conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
				return
			}

			item.stream = append(item.stream, id)
			store[key] = item
			mu.Unlock()

			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
		}
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}
