package event

import (
	"encoding/json"
)

// RowData represents a single row of data.
type RowData struct {
	// Field values (column name -> value)
	Fields map[string]Field `json:"fields"`
}

// Field represents a single field value.
type Field struct {
	// Field name
	Name string `json:"name"`

	// Field value
	Value interface{} `json:"value"`

	// Field type
	Type string `json:"type"`

	// Whether NULL
	Null bool `json:"null,omitempty"`
}

// NewRowData creates a new empty RowData.
func NewRowData() *RowData {
	return &RowData{
		Fields: make(map[string]Field),
	}
}

// Get returns a field value by name.
func (r *RowData) Get(name string) (interface{}, bool) {
	if r == nil {
		return nil, false
	}
	f, ok := r.Fields[name]
	if !ok {
		return nil, false
	}
	return f.Value, true
}

// GetField returns a Field by name.
func (r *RowData) GetField(name string) (Field, bool) {
	if r == nil {
		return Field{}, false
	}
	f, ok := r.Fields[name]
	return f, ok
}

// Set sets a field value.
func (r *RowData) Set(name string, value interface{}, typ string) {
	r.Fields[name] = Field{
		Name:  name,
		Value: value,
		Type:  typ,
		Null:  value == nil,
	}
}

// SetNull sets a field to NULL.
func (r *RowData) SetNull(name string, typ string) {
	r.Fields[name] = Field{
		Name:  name,
		Value: nil,
		Type:  typ,
		Null:  true,
	}
}

// Clone returns a deep copy of the RowData.
func (r *RowData) Clone() *RowData {
	if r == nil {
		return nil
	}
	result := &RowData{
		Fields: make(map[string]Field, len(r.Fields)),
	}
	for k, v := range r.Fields {
		result.Fields[k] = v
	}
	return result
}

// IsEmpty returns true if the RowData has no fields.
func (r *RowData) IsEmpty() bool {
	return r == nil || len(r.Fields) == 0
}

// MarshalJSON implements json.Marshaler.
func (r *RowData) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	type alias RowData
	return json.Marshal((*alias)(r))
}

// ColumnNames returns the names of all columns in order.
func (r *RowData) ColumnNames() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.Fields))
	for name := range r.Fields {
		names = append(names, name)
	}
	return names
}
