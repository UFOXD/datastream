package cli

import (
	"bytes"
	"testing"
)

func TestNewCLI(t *testing.T) {
	cli := New(nil)

	if cli == nil {
		t.Fatal("Expected non-nil CLI")
	}

	if cli.apiAddr != "http://localhost:8300" {
		t.Errorf("Expected default API addr, got '%s'", cli.apiAddr)
	}
}

func TestNewCLIWithConfig(t *testing.T) {
	cfg := &Config{
		APIAddr: "http://localhost:9000",
		Output:  &bytes.Buffer{},
	}

	cli := New(cfg)

	if cli.apiAddr != "http://localhost:9000" {
		t.Errorf("Expected API addr 'http://localhost:9000', got '%s'", cli.apiAddr)
	}
}

func TestBuildRootCommand(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildRootCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil root command")
	}

	if cmd.Use != "datastream-ctl" {
		t.Errorf("Expected use 'datastream-ctl', got '%s'", cmd.Use)
	}

	// Check subcommands exist
	subcmds := cmd.Commands()
	if len(subcmds) < 3 {
		t.Errorf("Expected at least 3 subcommands, got %d", len(subcmds))
	}
}

func TestBuildTaskCommand(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task command")
	}

	if cmd.Use != "task" {
		t.Errorf("Expected use 'task', got '%s'", cmd.Use)
	}

	// Check subcommands exist
	subcmds := cmd.Commands()
	if len(subcmds) < 6 {
		t.Errorf("Expected at least 6 task subcommands, got %d", len(subcmds))
	}
}

func TestBuildNodeCommand(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildNodeCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil node command")
	}

	if cmd.Use != "node" {
		t.Errorf("Expected use 'node', got '%s'", cmd.Use)
	}
}

func TestBuildVersionCommand(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildVersionCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil version command")
	}

	if cmd.Use != "version" {
		t.Errorf("Expected use 'version', got '%s'", cmd.Use)
	}
}

func TestVersionCommand(t *testing.T) {
	buf := &bytes.Buffer{}
	cli := New(&Config{Output: buf})

	cmd := cli.buildVersionCommand()
	cmd.Run(cmd, nil)

	output := buf.String()
	if output != "DataStream v1.0.0\n" {
		t.Errorf("Expected version output, got '%s'", output)
	}
}

func TestPrintJSON(t *testing.T) {
	buf := &bytes.Buffer{}
	cli := New(&Config{Output: buf})

	data := map[string]string{"key": "value"}
	if err := cli.printJSON(data); err != nil {
		t.Fatalf("printJSON failed: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Error("Expected non-empty output")
	}
}

func TestNewReader(t *testing.T) {
	r := NewReader("test")
	buf := make([]byte, 10)

	n, err := r.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != 4 {
		t.Errorf("Expected 4 bytes read, got %d", n)
	}

	if string(buf[:n]) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(buf[:n]))
	}
}

func TestStringReaderEOF(t *testing.T) {
	r := &stringReader{s: ""}
	buf := make([]byte, 10)

	_, err := r.Read(buf)
	if err.Error() != "EOF" {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestCLIConfig(t *testing.T) {
	cfg := &Config{
		APIAddr: "http://localhost:9000",
	}

	if cfg.APIAddr != "http://localhost:9000" {
		t.Errorf("Expected API addr 'http://localhost:9000', got '%s'", cfg.APIAddr)
	}
}

func TestTaskListCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskListCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task list command")
	}

	if cmd.Use != "list" {
		t.Errorf("Expected use 'list', got '%s'", cmd.Use)
	}
}

func TestTaskCreateCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskCreateCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task create command")
	}

	if cmd.Use != "create <id> <name>" {
		t.Errorf("Expected use 'create <id> <name>', got '%s'", cmd.Use)
	}
}

func TestTaskGetCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskGetCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task get command")
	}

	if cmd.Use != "get <id>" {
		t.Errorf("Expected use 'get <id>', got '%s'", cmd.Use)
	}
}

func TestTaskDeleteCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskDeleteCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task delete command")
	}

	if cmd.Use != "delete <id>" {
		t.Errorf("Expected use 'delete <id>', got '%s'", cmd.Use)
	}
}

func TestTaskStartCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskStartCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task start command")
	}

	if cmd.Use != "start <id>" {
		t.Errorf("Expected use 'start <id>', got '%s'", cmd.Use)
	}
}

func TestTaskStopCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTaskStopCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil task stop command")
	}

	if cmd.Use != "stop <id>" {
		t.Errorf("Expected use 'stop <id>', got '%s'", cmd.Use)
	}
}

func TestBuildTablesCommand(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables command")
	}

	if cmd.Use != "tables" {
		t.Errorf("Expected use 'tables', got '%s'", cmd.Use)
	}

	// Check subcommands exist
	subcmds := cmd.Commands()
	if len(subcmds) < 6 {
		t.Errorf("Expected at least 6 tables subcommands, got %d", len(subcmds))
	}
}

func TestTablesAddCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesAddCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables add command")
	}

	if cmd.Use != "add <db.table>..." {
		t.Errorf("Expected use 'add <db.table>...', got '%s'", cmd.Use)
	}
}

func TestTablesRemoveCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesRemoveCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables remove command")
	}

	if cmd.Use != "remove <db.table>..." {
		t.Errorf("Expected use 'remove <db.table>...', got '%s'", cmd.Use)
	}
}

func TestTablesListCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesListCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables list command")
	}

	if cmd.Use != "list" {
		t.Errorf("Expected use 'list', got '%s'", cmd.Use)
	}
}

func TestTablesGetCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesGetCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables get command")
	}

	if cmd.Use != "get <db.table>" {
		t.Errorf("Expected use 'get <db.table>', got '%s'", cmd.Use)
	}
}

func TestTablesPauseCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesPauseCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables pause command")
	}

	if cmd.Use != "pause <db.table>" {
		t.Errorf("Expected use 'pause <db.table>', got '%s'", cmd.Use)
	}
}

func TestTablesResumeCommandBuild(t *testing.T) {
	cli := New(nil)
	cmd := cli.buildTablesResumeCommand()

	if cmd == nil {
		t.Fatal("Expected non-nil tables resume command")
	}

	if cmd.Use != "resume <db.table>" {
		t.Errorf("Expected use 'resume <db.table>', got '%s'", cmd.Use)
	}
}
