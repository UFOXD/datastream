package cli

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/UFOXD/datastream/pkg/event"
)

// buildSchemaCommand builds the schema command.
func (c *CLI) buildSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Schema history tools",
	}

	cmd.AddCommand(c.buildSchemaHistoryCommand())

	return cmd
}

// buildSchemaHistoryCommand builds the schema history command.
func (c *CLI) buildSchemaHistoryCommand() *cobra.Command {
	var filePath string

	cmd := &cobra.Command{
		Use:   "history",
		Short: "Read and display schema history log file",
		Long:  "Opens a schema_history.log file and outputs each record as a JSON line to stdout",
		Example: `  # Display schema history records
  datastream-ctl schema history --file /path/to/meta/schema_history.log`,
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

			var count int
			for {
				var lenBuf [4]byte
				if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("read length: %w", err)
				}

				dataLen := binary.BigEndian.Uint32(lenBuf[:])
				data := make([]byte, dataLen)
				if _, err := io.ReadFull(f, data); err != nil {
					return fmt.Errorf("read data: %w", err)
				}

				var rec event.SchemaHistoryRecord
				if err := json.Unmarshal(data, &rec); err != nil {
					return fmt.Errorf("unmarshal record: %w", err)
				}

				if err := enc.Encode(&rec); err != nil {
					return fmt.Errorf("encode JSON: %w", err)
				}
				count++
			}

			fmt.Fprintf(os.Stderr, "Total records: %d\n", count)
			return nil
		},
	}

	cmd.Flags().StringVarP(&filePath, "file", "f", "", "Path to schema_history.log file")
	_ = cmd.MarkFlagRequired("file")

	return cmd
}
