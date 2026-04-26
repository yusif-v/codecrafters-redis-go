package main

import "strings"

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
