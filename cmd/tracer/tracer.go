package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fission/fission-workflows/test/benchmarks/tracer"
)

// tracer - prints lines according to a predefined temporal trace
func main() {
	var fileFormat string
	flag.StringVar(&fileFormat, "format", "JSON", "")
	flag.Parse()
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: tracer [--debug, --format <JSON>] <trace-file>\n")
		os.Exit(1)
	}

	// Set format
	var entryParser tracer.EntryParser
	switch strings.ToUpper(fileFormat) {
	case "JSON":
		entryParser = tracer.JSONEntryParser
	case "CSV":
		entryParser = tracer.CSVEntryParser
	default:
		log.Fatalf("Unknown file format '%v'", fileFormat)
	}
	filename := flag.Arg(0)

	// Load file
	fd, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	trace, err := tracer.Load(fd, entryParser)
	if err != nil {
		panic(err)
	}

	// Start trace
	traceTicker := tracer.Start(context.TODO(), trace)
	for {
		entry, ok := <-traceTicker
		if !ok {
			break
		}
		d, err := json.Marshal(entry.Payload)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n", string(d))
	}
}
