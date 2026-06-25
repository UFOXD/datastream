package cli

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

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

// readRecord reads one record from the file: [4B len][4B CRC][payload][4B tail_len].
// Returns the payload bytes or an error.
func readRecord(f *os.File) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])

	var crcBuf [4]byte
	if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("read crc: %w", err)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	var tailBuf [4]byte
	if _, err := io.ReadFull(f, tailBuf[:]); err != nil {
		return nil, fmt.Errorf("read tail: %w", err)
	}
	tailLen := binary.BigEndian.Uint32(tailBuf[:])
	if tailLen != payloadLen {
		return nil, fmt.Errorf("tail length mismatch: header=%d tail=%d", payloadLen, tailLen)
	}

	storedCRC := binary.BigEndian.Uint32(crcBuf[:])
	actualCRC := crc32.ChecksumIEEE(append(lenBuf[:], payload...))
	if storedCRC != actualCRC {
		return nil, fmt.Errorf("CRC32 mismatch: expected %08x got %08x", storedCRC, actualCRC)
	}

	return payload, nil
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
				payload, err := readRecord(f)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				ev, err := cache.UnmarshalCacheEvent(payload)
				if err != nil {
					return fmt.Errorf("unmarshal: %w", err)
				}

				jsonObj := map[string]interface{}{
					"source_type":  ev.SourceType,
					"tx_id":        ev.TxID,
					"event_seq":    ev.EventSeq,
					"is_begin":     ev.IsBegin,
					"is_commit":    ev.IsCommit,
					"payload_size": len(ev.Payload),
					"timestamp_ms": ev.TimestampMs,
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
		Long:  "Opens a binlog cache file and reports total events, unique tx_ids, total bytes, and time range",
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

			info, err := f.Stat()
			if err != nil {
				return fmt.Errorf("failed to stat file: %w", err)
			}
			totalBytes := info.Size()

			var (
				totalEvents int
				txIDs       = make(map[string]struct{})
				minTs       int64
				maxTs       int64
				firstEvent  = true
			)

			for {
				payload, err := readRecord(f)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				ev, err := cache.UnmarshalCacheEvent(payload)
				if err != nil {
					return fmt.Errorf("unmarshal: %w", err)
				}

				totalEvents++

				if ev.TxID != "" {
					txIDs[ev.TxID] = struct{}{}
				}

				ts := ev.TimestampMs
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

			fmt.Fprintf(c.output, "File:         %s\n", filePath)
			fmt.Fprintf(c.output, "Total bytes:  %d\n", totalBytes)
			fmt.Fprintf(c.output, "Total events: %d\n", totalEvents)
			fmt.Fprintf(c.output, "Unique TxIDs: %d\n", len(txIDs))

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
