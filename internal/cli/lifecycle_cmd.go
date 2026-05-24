package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// buildTaskDetailCommand builds the task detail command.
func (c *CLI) buildTaskDetailCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "detail <task-id>",
		Short: "Get task detail including table lifecycle states",
		Long:  "Get detailed task information including per-table lifecycle states and progress",
		Example: `  # Get task detail
  datastream-ctl task detail my-task-1`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]

			resp, err := c.client.Get(c.apiAddr + "/api/v1/tasks/" + taskID + "/detail")
			if err != nil {
				return fmt.Errorf("failed to get task detail: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' not found", taskID)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to get task detail: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	}
}

// buildTaskErrorsCommand builds the task errors command.
func (c *CLI) buildTaskErrorsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "errors <task-id>",
		Short: "List tables with errors in a task",
		Long:  "List all tables that have encountered errors during synchronization",
		Example: `  # List error tables
  datastream-ctl task errors my-task-1`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]

			resp, err := c.client.Get(c.apiAddr + "/api/v1/tasks/" + taskID + "/tables/errors")
			if err != nil {
				return fmt.Errorf("failed to get table errors: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' not found", taskID)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to get table errors: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	}
}

// buildTaskRestartTableCommand builds the task restart-table command.
func (c *CLI) buildTaskRestartTableCommand() *cobra.Command {
	var (
		schema string
		force  bool
	)

	cmd := &cobra.Command{
		Use:   "restart-table <task-id> [tables...]",
		Short: "Restart table synchronization",
		Long:  "Restart synchronization for specified tables. If no tables are specified, all tables are restarted.",
		Example: `  # Restart specific tables
  datastream-ctl task restart-table my-task-1 users orders

  # Restart tables in a specific schema
  datastream-ctl task restart-table my-task-1 users orders --schema mydb

  # Force restart
  datastream-ctl task restart-table my-task-1 --force`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			tables := args[1:]

			reqBody := map[string]interface{}{}
			if len(tables) > 0 {
				reqBody["tables"] = tables
			}
			if schema != "" {
				reqBody["schema"] = schema
			}
			if force {
				reqBody["force"] = true
			}

			resp, err := c.post("/api/v1/tasks/"+taskID+"/tables/restart", reqBody)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' not found", taskID)
			}
			if resp.StatusCode != http.StatusOK {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				if msg, ok := errResp["message"]; ok {
					return fmt.Errorf("failed to restart tables: %v", msg)
				}
				return fmt.Errorf("failed to restart tables: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			fmt.Fprintf(c.output, "Table restart initiated for task '%s'\n", taskID)
			return c.printJSON(result)
		},
	}

	cmd.Flags().StringVar(&schema, "schema", "", "Database schema to filter tables")
	cmd.Flags().BoolVar(&force, "force", false, "Force restart even if tables are running")

	return cmd
}

// buildTaskPauseTableCommand builds the task pause-table command.
func (c *CLI) buildTaskPauseTableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pause-table <task-id> <db.table>",
		Short: "Pause lifecycle of a table in a task",
		Long:  "Pause the lifecycle state machine for a specific table, halting its synchronization progress",
		Example: `  # Pause table lifecycle
  datastream-ctl task pause-table my-task-1 mydb.users`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			table := args[1]

			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'db.table'", table)
			}

			resp, err := c.post("/api/v1/tasks/"+taskID+"/tables/"+table+"/pause-lifecycle", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' or table '%s' not found", taskID, table)
			}
			if resp.StatusCode != http.StatusOK {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				if msg, ok := errResp["message"]; ok {
					return fmt.Errorf("failed to pause table: %v", msg)
				}
				return fmt.Errorf("failed to pause table: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Table '%s' lifecycle paused in task '%s'\n", table, taskID)
			return nil
		},
	}
}

// buildTaskResumeTableCommand builds the task resume-table command.
func (c *CLI) buildTaskResumeTableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "resume-table <task-id> <db.table>",
		Short: "Resume lifecycle of a paused table in a task",
		Long:  "Resume the lifecycle state machine for a previously paused table",
		Example: `  # Resume table lifecycle
  datastream-ctl task resume-table my-task-1 mydb.users`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			table := args[1]

			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'db.table'", table)
			}

			resp, err := c.post("/api/v1/tasks/"+taskID+"/tables/"+table+"/resume-lifecycle", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' or table '%s' not found", taskID, table)
			}
			if resp.StatusCode != http.StatusOK {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				if msg, ok := errResp["message"]; ok {
					return fmt.Errorf("failed to resume table: %v", msg)
				}
				return fmt.Errorf("failed to resume table: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Table '%s' lifecycle resumed in task '%s'\n", table, taskID)
			return nil
		},
	}
}

// buildTaskSkipErrorCommand builds the task skip-error command.
func (c *CLI) buildTaskSkipErrorCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "skip-error <task-id> <db.table>",
		Short: "Skip the current error for a table",
		Long:  "Skip the current error blocking a table's synchronization and continue processing",
		Example: `  # Skip error for a table
  datastream-ctl task skip-error my-task-1 mydb.users`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			table := args[1]

			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'db.table'", table)
			}

			resp, err := c.post("/api/v1/tasks/"+taskID+"/tables/"+table+"/skip-error", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' or table '%s' not found", taskID, table)
			}
			if resp.StatusCode != http.StatusOK {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				if msg, ok := errResp["message"]; ok {
					return fmt.Errorf("failed to skip error: %v", msg)
				}
				return fmt.Errorf("failed to skip error: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Error skipped for table '%s' in task '%s'\n", table, taskID)
			return nil
		},
	}
}

// buildTaskRetryTableCommand builds the task retry-table command.
func (c *CLI) buildTaskRetryTableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "retry-table <task-id> <db.table>",
		Short: "Retry a failed table",
		Long:  "Retry synchronization for a table that has encountered an error",
		Example: `  # Retry a failed table
  datastream-ctl task retry-table my-task-1 mydb.users`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			table := args[1]

			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'db.table'", table)
			}

			resp, err := c.post("/api/v1/tasks/"+taskID+"/tables/"+table+"/retry", nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' or table '%s' not found", taskID, table)
			}
			if resp.StatusCode != http.StatusOK {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				if msg, ok := errResp["message"]; ok {
					return fmt.Errorf("failed to retry table: %v", msg)
				}
				return fmt.Errorf("failed to retry table: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Retry initiated for table '%s' in task '%s'\n", table, taskID)
			return nil
		},
	}
}

// buildTaskTableLifecycleCommand builds the task table-lifecycle command.
func (c *CLI) buildTaskTableLifecycleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "table-lifecycle <task-id> <db.table>",
		Short: "Get lifecycle state of a table",
		Long:  "Get the current lifecycle state and history of a specific table in a task",
		Example: `  # Get table lifecycle
  datastream-ctl task table-lifecycle my-task-1 mydb.users`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			table := args[1]

			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'db.table'", table)
			}

			resp, err := c.client.Get(c.apiAddr + "/api/v1/tasks/" + taskID + "/tables/" + table + "/lifecycle")
			if err != nil {
				return fmt.Errorf("failed to get table lifecycle: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("task '%s' or table '%s' not found", taskID, table)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to get table lifecycle: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			return c.printJSON(result)
		},
	}
}
