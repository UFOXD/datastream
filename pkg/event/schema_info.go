package event

// SchemaInfo describes the schema of data.
type SchemaInfo struct {
	// Version number
	Version int64 `json:"version"`

	// Column definitions
	Columns []ColumnSchema `json:"columns"`

	// Primary key
	PrimaryKey []string `json:"primaryKey,omitempty"`
}

// ColumnSchema defines a column's schema.
type ColumnSchema struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	Optional bool        `json:"optional"`
	Default  interface{} `json:"default,omitempty"`
}

// GetColumn returns the ColumnSchema for a given column name.
func (s *SchemaInfo) GetColumn(name string) *ColumnSchema {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i]
		}
	}
	return nil
}

// Clone returns a deep copy of the SchemaInfo.
func (s *SchemaInfo) Clone() *SchemaInfo {
	if s == nil {
		return nil
	}
	result := &SchemaInfo{
		Version:    s.Version,
		Columns:    make([]ColumnSchema, len(s.Columns)),
		PrimaryKey: make([]string, len(s.PrimaryKey)),
	}
	copy(result.Columns, s.Columns)
	copy(result.PrimaryKey, s.PrimaryKey)
	return result
}

// IsCompatible checks if this schema is compatible with another.
// Returns true if the schemas are compatible for data migration.
func (s *SchemaInfo) IsCompatible(other *SchemaInfo) bool {
	if s == nil || other == nil {
		return false
	}

	// Build column map
	otherCols := make(map[string]ColumnSchema)
	for _, c := range other.Columns {
		otherCols[c.Name] = c
	}

	// Check all our columns exist in other schema with compatible type
	for _, c := range s.Columns {
		otherCol, ok := otherCols[c.Name]
		if !ok {
			// Column doesn't exist in target schema
			return false
		}
		// Check type compatibility (simplified)
		if c.Type != otherCol.Type {
			return false
		}
	}

	return true
}
