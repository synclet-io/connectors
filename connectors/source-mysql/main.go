package main

import (
	"log"
	"os"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

func main() {
	source := NewMySQLSource()
	runner := airbyte.NewSourceRunner(source, os.Stdout)

	if err := runner.Start(); err != nil {
		log.Fatal(err)
	}
}
