package main

import (
	"fmt"
	"strings"
)

func main() {
	text := "Hello, world! This is a test."
	words := strings.Fields(text)
	for i, word := range words {
		fmt.Printf("Word %d: %s\n", i+1, word)
	}
}
