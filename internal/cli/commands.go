// Package cli provides command-line interface for DataStream.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// CLI represents the command-line interface.
type CLI struct {
	rootCmd  *cobra.Command
	apiAddr  string
	client   *http.Client
	output   io.Writer
}

// Config holds CLI configuration.
type Config struct {
	APIAddr string
	Output  io.Writer
}

// New creates a new CLI.
func New(cfg *Config) *CLI {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.APIAddr == "" {
		cfg.APIAddr = "http://localhost:8300"
	}
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}

	cli := &CLI{
		apiAddr: cfg.APIAddr,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		output: cfg.Output,
	}

	cli.rootCmd = cli.buildRootCommand()
	return cli
}

// Execute runs the CLI.
func (c *CLI) Execute(args []string) error {
	c.rootCmd.SetArgs(args)
	return c.rootCmd.Execute()
}

// buildRootCommand builds the root command.
func (c *CLI) buildRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "datastream-ctl",
		Short: "DataStream control tool",
		Long:  "DataStream is a CDC platform for change data capture",
	}

	root.PersistentFlags().StringVarP(&c.apiAddr, "api-addr", "a", c.apiAddr, "API server address")

	// Add subcommands
	root.AddCommand(c.buildTaskCommand())
	root.AddCommand(c.buildNodeCommand())
	root.AddCommand(c.buildTablesCommand())
	root.AddCommand(c.buildBinlogCommand())
	root.AddCommand(c.buildVersionCommand())

	return root
}

// buildTaskCommand builds the task command.
func (c *CLI) buildTaskCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Manage tasks",
	}

	cmd.AddCommand(c.buildTaskListCommand())
	cmd.AddCommand(c.buildTaskCreateCommand())
	cmd.AddCommand(c.buildTaskGetCommand())
	cmd.AddCommand(c.buildTaskDeleteCommand())
	cmd.AddCommand(c.buildTaskStartCommand())
	cmd.AddCommand(c.buildTaskStopCommand())

	// Table lifecycle management commands
	cmd.AddCommand(c.buildTaskDetailCommand())
	cmd.AddCommand(c.buildTaskErrorsCommand())
	cmd.AddCommand(c.buildTaskRestartTableCommand())
	cmd.AddCommand(c.buildTaskPauseTableCommand())
	cmd.AddCommand(c.buildTaskResumeTableCommand())
	cmd.AddCommand(c.buildTaskSkipErrorCommand())
	cmd.AddCommand(c.buildTaskRetryTableCommand())
	cmd.AddCommand(c.buildTaskTableLifecycleCommand())

	return cmd
}

// buildTaskListCommand builds the task list command.
func (c *CLI) buildTaskListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := c.client.Get(c.apiAddr + "/api/v1/tasks")
			if err != nil {
				return fmt.Errorf("failed to list tasks: %w", err)
			}
			defer resp.Body.Close()

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	}
}

// buildTaskCreateCommand builds the task create command.
func (c *CLI) buildTaskCreateCommand() *cobra.Command {
	var configFile string

	cmd := &cobra.Command{
		Use:   "create <id> <name>",
		Short: "Create a task",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			name := args[1]

			// Read config file if provided
			var config map[string]interface{}
			if configFile != "" {
				data, err := os.ReadFile(configFile)
				if err != nil {
					return fmt.Errorf("failed to read config file: %w", err)
				}
				if err := json.Unmarshal(data, &config); err != nil {
					return fmt.Errorf("failed to parse config file: %w", err)
				}
			}

			req := map[string]interface{}{
				"id":     id,
				"name":   name,
				"config": config,
			}

			resp, err := c.post("/api/v1/tasks", req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			fmt.Fprintf(c.output, "Task '%s' created\n", id)
			return c.printJSON(result)
		},
	}

	cmd.Flags().StringVarP(&configFile, "config", "c", "", "Task configuration file")
	return cmd
}

// buildTaskGetCommand builds the task get command.
func (c *CLI) buildTaskGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get task details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			resp, err := c.client.Get(c.apiAddr + "/api/v1/tasks/" + id)
			if err != nil {
				return fmt.Errorf("failed to get task: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' not found", id)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	}
}

// buildTaskDeleteCommand builds the task delete command.
func (c *CLI) buildTaskDeleteCommand() *cobra.Command {
	force := false

	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			req, err := http.NewRequest(http.MethodDelete, c.apiAddr+"/api/v1/tasks/"+id, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}

			resp, err := c.client.Do(req)
			if err != nil {
				return fmt.Errorf("failed to delete task: %w", err)
			}
			defer resp.Body.Close()

			fmt.Fprintf(c.output, "Task '%s' deleted\n", id)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force delete even if running")
	return cmd
}

// buildTaskStartCommand builds the task start command.
func (c *CLI) buildTaskStartCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "start <id>",
		Short: "Start a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			_, err := c.post("/api/v1/tasks/"+id+"/start", nil)
			if err != nil {
				return err
			}

			fmt.Fprintf(c.output, "Task '%s' started\n", id)
			return nil
		},
	}
}

// buildTaskStopCommand builds the task stop command.
func (c *CLI) buildTaskStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop <id>",
		Short: "Stop a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			_, err := c.post("/api/v1/tasks/"+id+"/stop", nil)
			if err != nil {
				return err
			}

			fmt.Fprintf(c.output, "Task '%s' stopped\n", id)
			return nil
		},
	}
}

// buildNodeCommand builds the node command.
func (c *CLI) buildNodeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage nodes",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := c.client.Get(c.apiAddr + "/api/v1/nodes")
			if err != nil {
				return fmt.Errorf("failed to list nodes: %w", err)
			}
			defer resp.Body.Close()

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	})

	return cmd
}

// buildVersionCommand builds the version command.
func (c *CLI) buildVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(c.output, "DataStream v1.0.0")
		},
	}
}

// post sends a POST request.
func (c *CLI) post(path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = NewReader(string(data))
	}

	resp, err := c.client.Post(c.apiAddr+path, "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

// printJSON prints JSON output.
func (c *CLI) printJSON(data interface{}) error {
	enc := json.NewEncoder(c.output)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

// NewReader creates an io.Reader from a string.
func NewReader(s string) io.Reader {
	return &stringReader{s: s}
}

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

// ExecuteContext runs the CLI with context.
func ExecuteContext(ctx context.Context, args []string) error {
	cli := New(nil)
	return cli.Execute(args)
}
