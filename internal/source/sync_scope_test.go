package source

import (
	"testing"
)

func TestDatabaseScope_IsWildcardDatabase(t *testing.T) {
	tests := []struct {
		name  string
		scope DatabaseScope
		want  bool
	}{
		{
			name:  "wildcard mode with single asterisk",
			scope: DatabaseScope{Names: []string{"*"}},
			want:  true,
		},
		{
			name:  "single database returns false",
			scope: DatabaseScope{Names: []string{"db1"}},
			want:  false,
		},
		{
			name:  "multiple databases returns false",
			scope: DatabaseScope{Names: []string{"db1", "db2"}},
			want:  false,
		},
		{
			name:  "empty names returns false",
			scope: DatabaseScope{Names: []string{}},
			want:  false,
		},
		{
			name:  "asterisk with other entries returns false",
			scope: DatabaseScope{Names: []string{"*", "db1"}},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.scope.IsWildcardDatabase()
			if got != tt.want {
				t.Errorf("IsWildcardDatabase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatabaseScope_ShouldSyncDatabase(t *testing.T) {
	tests := []struct {
		name   string
		scope  DatabaseScope
		dbName string
		want   bool
	}{
		{
			name:   "wildcard matches any database",
			scope:  DatabaseScope{Names: []string{"*"}},
			dbName: "any_database",
			want:   true,
		},
		{
			name:   "wildcard matches another database",
			scope:  DatabaseScope{Names: []string{"*"}},
			dbName: "another_db",
			want:   true,
		},
		{
			name:   "exact match in list",
			scope:  DatabaseScope{Names: []string{"db1", "db2"}},
			dbName: "db1",
			want:   true,
		},
		{
			name:   "exact match second item",
			scope:  DatabaseScope{Names: []string{"db1", "db2"}},
			dbName: "db2",
			want:   true,
		},
		{
			name:   "no match returns false",
			scope:  DatabaseScope{Names: []string{"db1", "db2"}},
			dbName: "db3",
			want:   false,
		},
		{
			name:   "empty list returns false",
			scope:  DatabaseScope{Names: []string{}},
			dbName: "db1",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.scope.ShouldSyncDatabase(tt.dbName)
			if got != tt.want {
				t.Errorf("ShouldSyncDatabase(%q) = %v, want %v", tt.dbName, got, tt.want)
			}
		})
	}
}

func TestDatabaseScope_ShouldSyncTable(t *testing.T) {
	tests := []struct {
		name      string
		scope     DatabaseScope
		dbName    string
		tableName string
		want      bool
	}{
		{
			name:      "wildcard database matches all tables",
			scope:     DatabaseScope{Names: []string{"*"}},
			dbName:    "db1",
			tableName: "users",
			want:      true,
		},
		{
			name:      "wildcard database with no filter matches any table",
			scope:     DatabaseScope{Names: []string{"*"}},
			dbName:    "db2",
			tableName: "products",
			want:      true,
		},
		{
			name: "table filter pattern match",
			scope: DatabaseScope{
				Names:       []string{"db1"},
				TableFilter: []string{"db1\\.users"},
			},
			dbName:    "db1",
			tableName: "users",
			want:      true,
		},
		{
			name: "table filter pattern no match",
			scope: DatabaseScope{
				Names:       []string{"db1"},
				TableFilter: []string{"db1\\.orders"},
			},
			dbName:    "db1",
			tableName: "users",
			want:      false,
		},
		{
			name: "ignore table pattern excludes table",
			scope: DatabaseScope{
				Names:        []string{"db1"},
				IgnoreTables: []string{"db1\\.users"},
			},
			dbName:    "db1",
			tableName: "users",
			want:      false,
		},
		{
			name: "ignore table pattern does not exclude other tables",
			scope: DatabaseScope{
				Names:        []string{"db1"},
				IgnoreTables: []string{"db1\\.users"},
			},
			dbName:    "db1",
			tableName: "orders",
			want:      true,
		},
		{
			name:      "database not in scope returns false",
			scope:     DatabaseScope{Names: []string{"db1"}},
			dbName:    "db2",
			tableName: "users",
			want:      false,
		},
		{
			name: "filter pattern with regex prefix match",
			scope: DatabaseScope{
				Names:       []string{"db1"},
				TableFilter: []string{"db1\\..*"},
			},
			dbName:    "db1",
			tableName: "anything",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.scope.ShouldSyncTable(tt.dbName, tt.tableName)
			if got != tt.want {
				t.Errorf("ShouldSyncTable(%q, %q) = %v, want %v", tt.dbName, tt.tableName, got, tt.want)
			}
		})
	}
}

func TestTableScope_ShouldSyncTable(t *testing.T) {
	scope := TableScope{
		Names: []string{"db1.users", "db1.orders", "db2.products"},
	}

	tests := []struct {
		name      string
		dbName    string
		tableName string
		want      bool
	}{
		{
			name:      "db1.users is in scope",
			dbName:    "db1",
			tableName: "users",
			want:      true,
		},
		{
			name:      "db1.orders is in scope",
			dbName:    "db1",
			tableName: "orders",
			want:      true,
		},
		{
			name:      "db2.products is in scope",
			dbName:    "db2",
			tableName: "products",
			want:      true,
		},
		{
			name:      "db2.users is not in scope",
			dbName:    "db2",
			tableName: "users",
			want:      false,
		},
		{
			name:      "db1.products is not in scope",
			dbName:    "db1",
			tableName: "products",
			want:      false,
		},
		{
			name:      "unknown database is not in scope",
			dbName:    "db3",
			tableName: "users",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scope.ShouldSyncTable(tt.dbName, tt.tableName)
			if got != tt.want {
				t.Errorf("ShouldSyncTable(%q, %q) = %v, want %v", tt.dbName, tt.tableName, got, tt.want)
			}
		})
	}
}

func TestTableScope_ParseTableNames(t *testing.T) {
	tests := []struct {
		name  string
		scope TableScope
		want  []TableScopeEntry
	}{
		{
			name: "parses multiple database.table entries",
			scope: TableScope{
				Names: []string{"db1.users", "db1.orders", "db2.products"},
			},
			want: []TableScopeEntry{
				{Database: "db1", Table: "users"},
				{Database: "db1", Table: "orders"},
				{Database: "db2", Table: "products"},
			},
		},
		{
			name: "skips entries without dot separator",
			scope: TableScope{
				Names: []string{"db1.users", "invalid", "db2.products"},
			},
			want: []TableScopeEntry{
				{Database: "db1", Table: "users"},
				{Database: "db2", Table: "products"},
			},
		},
		{
			name:  "empty names returns nil",
			scope: TableScope{Names: []string{}},
			want:  nil,
		},
		{
			name: "table name with dot only splits on first dot",
			scope: TableScope{
				Names: []string{"db1.schema.table"},
			},
			want: []TableScopeEntry{
				{Database: "db1", Table: "schema.table"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.scope.ParseTableNames()
			if len(got) != len(tt.want) {
				t.Fatalf("ParseTableNames() returned %d entries, want %d", len(got), len(tt.want))
			}
			for i, entry := range got {
				if entry.Database != tt.want[i].Database {
					t.Errorf("entry[%d].Database = %q, want %q", i, entry.Database, tt.want[i].Database)
				}
				if entry.Table != tt.want[i].Table {
					t.Errorf("entry[%d].Table = %q, want %q", i, entry.Table, tt.want[i].Table)
				}
			}
		})
	}
}

func TestSyncScope_Validate(t *testing.T) {
	tests := []struct {
		name    string
		scope   SyncScope
		wantErr bool
	}{
		{
			name: "valid database scope",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names: []string{"db1"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid database scope wildcard",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names: []string{"*"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid table scope",
			scope: SyncScope{
				Level: SyncLevelTable,
				Tables: TableScope{
					Names: []string{"db1.users"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid empty database scope",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names: []string{},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid empty table scope",
			scope: SyncScope{
				Level: SyncLevelTable,
				Tables: TableScope{
					Names: []string{},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid level",
			scope: SyncScope{
				Level: SyncLevel("invalid"),
			},
			wantErr: true,
		},
		{
			name: "invalid regex in table filter",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names:       []string{"db1"},
					TableFilter: []string{"[invalid"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scope.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
