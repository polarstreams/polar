package main

import (
	"fmt"

	"github.com/jorgebay/soda/internal/localdb"
)

func main() {
	fmt.Println("Starting Soda")
	fmt.Println("Initializing local db")
	localDbClient := localdb.NewClient()

	if localDbClient.Init() != nil {
		// TODO
	}

	fmt.Println("Soda started")
}
