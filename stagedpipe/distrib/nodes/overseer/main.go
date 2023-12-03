package main

import (
	"log"

	osFS "github.com/gopherfs/fs/io/os"
)

func main() {
	fsys, err := osFS.New()
	if err != nil {
		log.Fatalf("failed to create os fs: %w", err)
	}
}
