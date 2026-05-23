package cli

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/UFOXD/datastream/internal/cache"
)

// buildBinlogCommand builds the binlog command.
func (c *CLI) buildBinlogCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlog",
		Short: "Binlog cache tools",
	}

	cmd.AddCommand(c.buildBinlogDecodeCommand())
	cmd.AddCommand(c.buildBinlogStatCommand())

	return cmd
}

// buildBinlogDecodeCommand builds the binlog decode command.
func (c *CLI) buildBinlogDecodeCommand() *cobra.Command {
	var (
		filePath string
		format   string
	)

	cmd := &cobra.Command{
		Use:   "decode",
		Short: "Decode binlog cache file to JSON",
		Long:  "Opens a binlog cache file and outputs each CacheEvent as a JSON line to stdout",
		Example: `  # Decode a binlog cache file
  datastream-ctl binlog decode --file /path/to/file.binlog

  # Specify output format (only json supported currently)
  datastream-ctl binlog decode --file /path/to/file.binlog --format json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if filePath == "" {
				return fmt.Errorf("--file is required")
			}

			f, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer f.Close()

			enc := json.NewEncoder(c.output)

			for {
				// Read 4-byte length prefix (big-endian uint32)
				var length uint32
				if err := binary.Read(f, binary.BigEndian, &length); err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to read length prefix: %w", err)
				}

				// Read the payload
				payload := make([]byte, length)
				if _, err := io.ReadFull(f, payload); err != nil {
					return fmt.Errorf("failed to read payload (%d bytes): %w", length, err)
				}

				// Unmarshal protobuf
				event := &cache.CacheEvent{}
				if err := proto.Unmarshal(payload, event); err != nil {
					return fmt.Errorf("failed to unmarshal CacheEvent: %w", err)
				}

				// Output as JSON line
				jsonObj := map[string]interface{}{
					"gtid":         event.GetGtid(),
					"event_seq":    event.GetEventSeq(),
					"is_begin":     event.GetIsBegin(),
					"is_commit":    event.GetIsCommit(),
					"payload_size": len(event.GetPayload()),
					"timestamp_ms": event.GetTimestampMs(),
				}

				if err := enc.Encode(jsonObj); err != nil {
					return fmt.Errorf("failed to encode JSON: %w", err)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&filePath, "file", "f", "", "Path to binlog cache file")
	cmd.Flags().StringVar(&format, "format", "json", "Output format (json)")
	_ = cmd.MarkFlagRequired("file")

	return cmd
}

// buildBinlogStatCommand builds the binlog stat command.
func (c *CLI) buildBinlogStatCommand() *cobra.Command {
	var filePath string

	cmd := &cobra.Command{
		Use:   "stat",
		Short: "Show statistics for a binlog cache file",
		Long:  "Opens a binlog cache file and reports total events, unique GTIDs, total bytes, and time range",
		Example: `  # Show stats for a binlog cache file
  datastream-ctl binlog stat --file /path/to/file.binlog`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if filePath == "" {
				return fmt.Errorf("--file is required")
			}

			f, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer f.Close()

			// Get file size
			info, err := f.Stat()
			if err != nil {
				return fmt.Errorf("failed to stat file: %w", err)
			}
			totalBytes := info.Size()

			var (
				totalEvents int
				gtids       = make(map[string]struct{})
				minTs       int64
				maxTs       int64
				firstEvent  = true
			)

			for {
				// Read 4-byte length prefix (big-endian uint32)
				var length uint32
				if err := binary.Read(f, binary.BigEndian, &length); err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("failed to read length prefix: %w", err)
				}

				// Read the payload
				payload := make([]byte, length)
				if _, err := io.ReadFull(f, payload); err != nil {
					return fmt.Errorf("failed to read payload (%d bytes): %w", length, err)
				}

				// Unmarshal protobuf
				event := &cache.CacheEvent{}
				if err := proto.Unmarshal(payload, event); err != nil {
					return fmt.Errorf("failed to unmarshal CacheEvent: %w", err)
				}

				totalEvents++

				if gtid := event.GetGtid(); gtid != "" {
					gtids[gtid] = struct{}{}
				}

				ts := event.GetTimestampMs()
				if firstEvent {
					minTs = ts
					maxTs = ts
					firstEvent = false
				} else {
					if ts < minTs {
						minTs = ts
					}
					if ts > maxTs {
						maxTs = ts
					}
				}
			}

			// Print summary
			fmt.Fprintf(c.output, "File:         %s\n", filePath)
			fmt.Fprintf(c.output, "Total bytes:  %d\n", totalBytes)
			fmt.Fprintf(c.output, "Total events: %d\n", totalEvents)
			fmt.Fprintf(c.output, "Unique GTIDs: %d\n", len(gtids))

			if totalEvents > 0 {
				earliest := time.UnixMilli(minTs)
				latest := time.UnixMilli(maxTs)
				fmt.Fprintf(c.output, "Time range:   %s -> %s\n",
					earliest.Format(time.RFC3339),
					latest.Format(time.RFC3339),
				)
			} else {
				fmt.Fprintf(c.output, "Time range:   (no events)\n")
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&filePath, "file", "f", "", "Path to binlog cache file")
	_ = cmd.MarkFlagRequired("file")

	return cmd
}
