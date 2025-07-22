package main

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/internal/ee/app"
)

func main() {
	fmt.Println("Testing EE Transaction Generator...")
	
	if err := app.RunTxGeneratorExample(); err != nil {
		log.Fatalf("Generator test failed: %v", err)
	}
	
	fmt.Println("Generator test completed successfully!")
}