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
