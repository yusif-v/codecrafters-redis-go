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
	expiry time.Time
}

var store = map[string]entry{}
var mu sync.Mutex

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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading buffer: ", err.Error())
			return
		}

		parts := parseRESP(string(buf))
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
			if len(parts) > 3 {
				for _, val := range parts[2:] {
					item.list = append(item.list, val)
				}
			}
			store[parts[1]] = item
			mu.Unlock()
			fmt.Fprintf(conn, ":%d\r\n", len(item.list))
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
