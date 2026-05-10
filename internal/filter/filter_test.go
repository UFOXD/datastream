package filter

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPassAllFilter(t *testing.T) {
	f := NewPassAllFilter()

	tests := []struct {
		name    string
		event   *event.ChangeEvent
		expect  bool
		wantErr bool
	}{
		{
			name:   "insert event",
			event:  &event.ChangeEvent{Type: event.EventTypeInsert},
			expect: true,
		},
		{
			name:   "update event",
			event:  &event.ChangeEvent{Type: event.EventTypeUpdate},
			expect: true,
		},
		{
			name:   "delete event",
			event:  &event.ChangeEvent{Type: event.EventTypeDelete},
			expect: true,
		},
		{
			name:   "ddl event",
			event:  &event.ChangeEvent{Type: event.EventTypeDDL},
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass, err := f.Filter(tt.event)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			}
		})
	}
}

func TestBlockAllFilter(t *testing.T) {
	f := NewBlockAllFilter()

	tests := []struct {
		name    string
		event   *event.ChangeEvent
		expect  bool
		wantErr bool
	}{
		{
			name:   "insert event",
			event:  &event.ChangeEvent{Type: event.EventTypeInsert},
			expect: false,
		},
		{
			name:   "update event",
			event:  &event.ChangeEvent{Type: event.EventTypeUpdate},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass, err := f.Filter(tt.event)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			}
		})
	}
}

func TestFilterChain(t *testing.T) {
	t.Run("empty chain passes all", func(t *testing.T) {
		fc := NewFilterChain()
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		pass, err := fc.Filter(e)
		assert.NoError(t, err)
		assert.True(t, pass)
	})

	t.Run("single filter", func(t *testing.T) {
		fc := NewFilterChain(NewPassAllFilter())
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		pass, err := fc.Filter(e)
		assert.NoError(t, err)
		assert.True(t, pass)
	})

	t.Run("multiple filters - all pass", func(t *testing.T) {
		fc := NewFilterChain(
			NewPassAllFilter(),
			NewPassAllFilter(),
			NewPassAllFilter(),
		)
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		pass, err := fc.Filter(e)
		assert.NoError(t, err)
		assert.True(t, pass)
	})

	t.Run("multiple filters - one blocks", func(t *testing.T) {
		fc := NewFilterChain(
			NewPassAllFilter(),
			NewBlockAllFilter(),
			NewPassAllFilter(),
		)
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		pass, err := fc.Filter(e)
		assert.NoError(t, err)
		assert.False(t, pass)
	})

	t.Run("add filter", func(t *testing.T) {
		fc := NewFilterChain(NewPassAllFilter())
		fc.Add(NewBlockAllFilter())
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		pass, err := fc.Filter(e)
		assert.NoError(t, err)
		assert.False(t, pass)
	})
}

func TestRuleFilter(t *testing.T) {
	t.Run("no rules passes all", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{})
		require.NoError(t, err)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "testtable",
			},
		}

		pass, err := f.Filter(e)
		assert.NoError(t, err)
		assert.True(t, pass)
	})

	t.Run("include tables", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			IncludeTables: []string{"^testdb\\.users$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name   string
			db     string
			table  string
			expect bool
		}{
			{"matching table", "testdb", "users", true},
			{"non-matching table", "testdb", "orders", false},
			{"different database", "otherdb", "users", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: event.EventTypeInsert,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    tt.table,
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("exclude tables", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			ExcludeTables: []string{"^testdb\\.temp_.*$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name   string
			db     string
			table  string
			expect bool
		}{
			{"regular table", "testdb", "users", true},
			{"temp table", "testdb", "temp_data", false},
			{"another temp", "testdb", "temp_backup", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: event.EventTypeInsert,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    tt.table,
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("include databases", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			IncludeDatabases: []string{"^prod_.*$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name   string
			db     string
			expect bool
		}{
			{"prod database", "prod_main", true},
			{"another prod", "prod_backup", true},
			{"dev database", "dev_main", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: event.EventTypeInsert,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    "users",
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("exclude databases", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			ExcludeDatabases: []string{"^_.*$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name   string
			db     string
			expect bool
		}{
			{"hidden database", "_internal", false},
			{"regular database", "main", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: event.EventTypeInsert,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    "users",
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("include event types", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			IncludeTypes: []event.EventType{event.EventTypeInsert, event.EventTypeUpdate},
		})
		require.NoError(t, err)

		tests := []struct {
			name      string
			eventType event.EventType
			expect    bool
		}{
			{"insert", event.EventTypeInsert, true},
			{"update", event.EventTypeUpdate, true},
			{"delete", event.EventTypeDelete, false},
			{"ddl", event.EventTypeDDL, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{Type: tt.eventType}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("exclude event types", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			ExcludeTypes: []event.EventType{event.EventTypeDDL, event.EventTypeTruncate},
		})
		require.NoError(t, err)

		tests := []struct {
			name      string
			eventType event.EventType
			expect    bool
		}{
			{"insert", event.EventTypeInsert, true},
			{"update", event.EventTypeUpdate, true},
			{"delete", event.EventTypeDelete, true},
			{"ddl", event.EventTypeDDL, false},
			{"truncate", event.EventTypeTruncate, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{Type: tt.eventType}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})

	t.Run("combined rules", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			IncludeTables:    []string{"^prod\\..*$"},
			ExcludeTables:    []string{".*\\.temp_.*$"},
			IncludeTypes:     []event.EventType{event.EventTypeInsert, event.EventTypeUpdate},
			ExcludeDatabases: []string{"^test$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name      string
			db        string
			table     string
			eventType event.EventType
			expect    bool
		}{
			{"valid insert", "prod", "users", event.EventTypeInsert, true},
			{"valid update", "prod", "orders", event.EventTypeUpdate, true},
			{"excluded type", "prod", "users", event.EventTypeDelete, false},
			{"excluded database", "test", "users", event.EventTypeInsert, false},
			{"temp table", "prod", "temp_users", event.EventTypeInsert, false},
			{"wrong database", "dev", "users", event.EventTypeInsert, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: tt.eventType,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    tt.table,
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass, tt.name)
			})
		}
	})

	t.Run("wildcard patterns", func(t *testing.T) {
		f, err := NewRuleFilter(&Config{
			IncludeTables: []string{".*\\.users$"},
		})
		require.NoError(t, err)

		tests := []struct {
			name   string
			db     string
			table  string
			expect bool
		}{
			{"users in any db", "anydb", "users", true},
			{"users in another db", "otherdb", "users", true},
			{"different table", "anydb", "orders", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				e := &event.ChangeEvent{
					Type: event.EventTypeInsert,
					Table: event.TableInfo{
						Database: tt.db,
						Table:    tt.table,
					},
				}

				pass, err := f.Filter(e)
				assert.NoError(t, err)
				assert.Equal(t, tt.expect, pass)
			})
		}
	})
}

func TestRuleFilterAddMethods(t *testing.T) {
	f, err := NewRuleFilter(&Config{})
	require.NoError(t, err)

	// Test AddIncludeTable
	err = f.AddIncludeTable("^test\\.table$")
	assert.NoError(t, err)

	// Test AddExcludeTable
	err = f.AddExcludeTable("^test\\.temp$")
	assert.NoError(t, err)

	// Test AddIncludeType
	f.AddIncludeType(event.EventTypeInsert)

	// Test AddExcludeType
	f.AddExcludeType(event.EventTypeDelete)

	// Verify the filter works with added rules
	e := &event.ChangeEvent{
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test",
			Table:    "table",
		},
	}

	pass, err := f.Filter(e)
	assert.NoError(t, err)
	assert.True(t, pass)

	// Test excluded table
	e.Table.Table = "temp"
	pass, err = f.Filter(e)
	assert.NoError(t, err)
	assert.False(t, pass)

	// Test excluded type
	e.Table.Table = "table"
	e.Type = event.EventTypeDelete
	pass, err = f.Filter(e)
	assert.NoError(t, err)
	assert.False(t, pass)
}
