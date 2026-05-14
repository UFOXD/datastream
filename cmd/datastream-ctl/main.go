package main

import (
	"fmt"
	"os"

	"github.com/UFOXD/datastream/internal/cli"
	"github.com/UFOXD/datastream/pkg/version"
)

var (
	// Version is set at build time
	Version = "unknown"
	// BuildTime is set at build time
	BuildTime = "unknown"
)

func main() {
	// Set version info
	version.Version = Version
	version.BuildTime = BuildTime

	// Create and execute CLI
	c := cli.New(&cli.Config{
		APIAddr: os.Getenv("DATASTREAM_API_ADDR"),
	})

	if err := c.Execute(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
