package main

import (
	"log"
	"os"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func main() {
	runner := synclet.NewSourceRunner(NewExampleSource(), os.Stdout)

	if err := runner.Start(); err != nil {
		log.Fatal(err)
	}
}
