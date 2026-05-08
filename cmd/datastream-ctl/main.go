package main

import (
	"fmt"
	"os"

	"github.com/UFOXD/datastream/pkg/version"
)

var (
	// Version is set at build time
	Version = "unknown"
	// BuildTime is set at build time
	BuildTime = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "version":
		fmt.Println(version.GetInfo().String())
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf("DataStream CLI %s (built at %s)\n", version.Version, version.BuildTime)
	fmt.Println()
	fmt.Println("Usage: datastream-ctl [command]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  task      Task management (TODO)")
	fmt.Println("  node      Node management (TODO)")
	fmt.Println("  cluster   Cluster management (TODO)")
	fmt.Println("  version   Show version")
	fmt.Println("  help      Show this help")
}
