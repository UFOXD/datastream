package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

// buildTablesCommand builds the tables command.
func (c *CLI) buildTablesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tables",
		Short: "Manage sync tables",
		Long:  "Manage tables in the sync scope (add, remove, list, pause, resume)",
	}

	cmd.AddCommand(c.buildTablesAddCommand())
	cmd.AddCommand(c.buildTablesRemoveCommand())
	cmd.AddCommand(c.buildTablesListCommand())
	cmd.AddCommand(c.buildTablesGetCommand())
	cmd.AddCommand(c.buildTablesPauseCommand())
	cmd.AddCommand(c.buildTablesResumeCommand())

	return cmd
}

// buildTablesAddCommand builds the tables add command.
func (c *CLI) buildTablesAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "add <db.table>...",
		Short: "Add tables to sync scope",
		Long:  "Add one or more tables to the sync scope. Format: database.table",
		Example: `  # Add a single table
  datastream-ctl tables add mydb.users

  # Add multiple tables
  datastream-ctl tables add mydb.users mydb.orders mydb.products`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate table format
			for _, table := range args {
				if !strings.Contains(table, ".") {
					return fmt.Errorf("invalid table format '%s': expected 'database.table'", table)
				}
			}

			req := map[string]interface{}{
				"tables": args,
			}

			resp, err := c.post("/api/v1/tables", req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusConflict {
				return fmt.Errorf("one or more tables already exist in sync scope")
			}
			if resp.StatusCode == http.StatusBadRequest {
				var errResp map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&errResp)
				return fmt.Errorf("invalid request: %v", errResp["error"])
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to add tables: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Added %d table(s) to sync scope\n", len(args))
			return nil
		},
	}
}

// buildTablesRemoveCommand builds the tables remove command.
func (c *CLI) buildTablesRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "remove <db.table>...",
		Short: "Remove tables from sync scope",
		Long:  "Remove one or more tables from the sync scope. Format: database.table",
		Example: `  # Remove a single table
  datastream-ctl tables remove mydb.users

  # Remove multiple tables
  datastream-ctl tables remove mydb.users mydb.orders`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate table format
			for _, table := range args {
				if !strings.Contains(table, ".") {
					return fmt.Errorf("invalid table format '%s': expected 'database.table'", table)
				}
			}

			req := map[string]interface{}{
				"tables": args,
			}

			resp, err := c.post("/api/v1/tables/remove", req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("one or more tables not found in sync scope")
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to remove tables: HTTP %d", resp.StatusCode)
			}

			fmt.Fprintf(c.output, "Removed %d table(s) from sync scope\n", len(args))
			return nil
		},
	}
}

// buildTablesListCommand builds the tables list command.
func (c *CLI) buildTablesListCommand() *cobra.Command {
	var database string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all tables in sync scope",
		Long:  "List all tables currently in the sync scope with their status",
		Example: `  # List all tables
  datastream-ctl tables list

  # Filter by database
  datastream-ctl tables list --database mydb`,
		RunE: func(cmd *cobra.Command, args []string) error {
			url := "/api/v1/tables"
			if database != "" {
				url = url + "?database=" + database
			}

			resp, err := c.client.Get(c.apiAddr + url)
			if err != nil {
				return fmt.Errorf("failed to list tables: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to list tables: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			tables, ok := result["tables"].([]interface{})
			if !ok {
				return c.printJSON(result)
			}

			// Print in a formatted way
			fmt.Fprintf(c.output, "Tables in sync scope (%d total):\n\n", len(tables))
			for _, t := range tables {
				table, ok := t.(map[string]interface{})
				if !ok {
					continue
				}
				db, _ := table["database"].(string)
				tbl, _ := table["table"].(string)
				status, _ := table["status"].(string)
				fmt.Fprintf(c.output, "  %s.%s\t[%s]\n", db, tbl, status)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&database, "database", "d", "", "Filter by database name")
	return cmd
}

// buildTablesGetCommand builds the tables get command.
func (c *CLI) buildTablesGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <db.table>",
		Short: "Get sync state of a table",
		Long:  "Get detailed sync state information for a specific table",
		Example: `  # Get state of a table
  datastream-ctl tables get mydb.users`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			table := args[0]
			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'database.table'", table)
			}

			parts := strings.SplitN(table, ".", 2)
			db, tbl := parts[0], parts[1]

			resp, err := c.client.Get(c.apiAddr + "/api/v1/tables/" + db + "/" + tbl)
			if err != nil {
				return fmt.Errorf("failed to get table state: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("table '%s' not found in sync scope", table)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to get table state: HTTP %d", resp.StatusCode)
			}

			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}

			// Print formatted output
			fmt.Fprintf(c.output, "Table: %s.%s\n", result["database"], result["table"])
			fmt.Fprintf(c.output, "Status: %s\n", result["status"])
			if addedAt, ok := result["added_at"]; ok {
				fmt.Fprintf(c.output, "Added at: %v\n", addedAt)
			}
			if syncStarted, ok := result["sync_started"]; ok {
				fmt.Fprintf(c.output, "Sync started: %v\n", syncStarted)
			}
			if pausedAt, ok := result["paused_at"]; ok {
				fmt.Fprintf(c.output, "Paused at: %v\n", pausedAt)
			}
			if errStr, ok := result["error"]; ok && errStr != nil {
				fmt.Fprintf(c.output, "Error: %v\n", errStr)
			}

			return nil
		},
	}
}

// buildTablesPauseCommand builds the tables pause command.
func (c *CLI) buildTablesPauseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "pause <db.table>",
		Short: "Pause syncing of a table",
		Long:  "Pause the synchronization of a specific table. The table will remain in the sync scope but no changes will be captured.",
		Example: `  # Pause a table
  datastream-ctl tables pause mydb.users`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			table := args[0]
			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'database.table'", table)
			}

			parts := strings.SplitN(table, ".", 2)
			db, tbl := parts[0], parts[1]

			_, err := c.post("/api/v1/tables/"+db+"/"+tbl+"/pause", nil)
			if err != nil {
				return err
			}

			fmt.Fprintf(c.output, "Table '%s' paused\n", table)
			return nil
		},
	}
}

// buildTablesResumeCommand builds the tables resume command.
func (c *CLI) buildTablesResumeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <db.table>",
		Short: "Resume syncing of a paused table",
		Long:  "Resume the synchronization of a previously paused table.",
		Example: `  # Resume a table
  datastream-ctl tables resume mydb.users`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			table := args[0]
			if !strings.Contains(table, ".") {
				return fmt.Errorf("invalid table format '%s': expected 'database.table'", table)
			}

			parts := strings.SplitN(table, ".", 2)
			db, tbl := parts[0], parts[1]

			_, err := c.post("/api/v1/tables/"+db+"/"+tbl+"/resume", nil)
			if err != nil {
				return err
			}

			fmt.Fprintf(c.output, "Table '%s' resumed\n", table)
			return nil
		},
	}
}
