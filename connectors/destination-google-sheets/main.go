package main

import (
	"fmt"
	"os"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

func main() {
	dest := NewGoogleSheetsDestination()
	if err := airbyte.NewDestinationRunner(dest, os.Stdout).Start(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
