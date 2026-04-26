package main

import (
	"strconv"
	"strings"
)

func parseRESP(input string) []string {
	lines := strings.Split(input, "\r\n")
	if len(lines) == 0 || len(lines[0]) == 0 || lines[0][0] != '*' {
		return nil
	}
	count, err := strconv.Atoi(lines[0][1:])
	if err != nil || count <= 0 {
		return nil
	}

	result := make([]string, 0, count)
	i := 1
	for j := 0; j < count && i < len(lines); j++ {
		if len(lines[i]) == 0 || lines[i][0] != '$' {
			return nil
		}
		i++
		if i < len(lines) {
			result = append(result, lines[i])
			i++
		}
	}
	return result
}
