package transform

import (
	"errors"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityTransformer(t *testing.T) {
	tr := NewIdentityTransformer()

	e := &event.ChangeEvent{
		Type: event.EventTypeInsert,
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 1, Type: "int"},
				"name": {Name: "name", Value: "test", Type: "varchar"},
			},
		},
	}

	result, err := tr.Transform(e)
	require.NoError(t, err)
	assert.Equal(t, e, result)
}

func TestTransformChain(t *testing.T) {
	t.Run("empty chain passes through", func(t *testing.T) {
		tc := NewTransformChain()
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		result, err := tc.Transform(e)
		assert.NoError(t, err)
		assert.Equal(t, e, result)
	})

	t.Run("single transformer", func(t *testing.T) {
		tc := NewTransformChain(NewIdentityTransformer())
		e := &event.ChangeEvent{Type: event.EventTypeInsert}

		result, err := tc.Transform(e)
		assert.NoError(t, err)
		assert.Equal(t, e, result)
	})

	t.Run("multiple transformers in order", func(t *testing.T) {
		// Create transformers that add different static fields
		t1 := NewMappingTransformer(&MappingConfig{
			StaticFields: map[string]interface{}{"field1": "value1"},
		})
		t2 := NewMappingTransformer(&MappingConfig{
			StaticFields: map[string]interface{}{"field2": "value2"},
		})

		tc := NewTransformChain(t1, t2)
		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: 1, Type: "int"},
				},
			},
		}

		result, err := tc.Transform(e)
		assert.NoError(t, err)

		// Both static fields should be present
		_, ok1 := result.After.Fields["field1"]
		_, ok2 := result.After.Fields["field2"]
		assert.True(t, ok1)
		assert.True(t, ok2)
	})

	t.Run("add transformer", func(t *testing.T) {
		tc := NewTransformChain(NewIdentityTransformer())
		tc.Add(NewIdentityTransformer())

		e := &event.ChangeEvent{Type: event.EventTypeInsert}
		result, err := tc.Transform(e)
		assert.NoError(t, err)
		assert.Equal(t, e, result)
	})

	t.Run("transformers returns list", func(t *testing.T) {
		t1 := NewIdentityTransformer()
		t2 := NewIdentityTransformer()
		tc := NewTransformChain(t1, t2)

		transformers := tc.Transformers()
		assert.Len(t, transformers, 2)
	})
}

func TestMappingTransformer(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		mt := NewMappingTransformer(nil)
		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: 1, Type: "int"},
				},
			},
		}

		result, err := mt.Transform(e)
		assert.NoError(t, err)
		assert.Equal(t, e, result)
	})

	t.Run("field mapping", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			FieldMapping: map[string]string{
				"old_name": "new_name",
				"id":       "user_id",
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":       {Name: "id", Value: 1, Type: "int"},
					"old_name": {Name: "old_name", Value: "test", Type: "varchar"},
					"keep":     {Name: "keep", Value: "value", Type: "varchar"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		// Check mapped fields
		_, hasOldID := result.After.Fields["id"]
		_, hasNewID := result.After.Fields["user_id"]
		assert.False(t, hasOldID, "old 'id' field should be renamed")
		assert.True(t, hasNewID, "should have 'user_id' field")

		_, hasOldName := result.After.Fields["old_name"]
		_, hasNewName := result.After.Fields["new_name"]
		assert.False(t, hasOldName, "old 'old_name' field should be renamed")
		assert.True(t, hasNewName, "should have 'new_name' field")

		// Unmapped field should remain
		_, hasKeep := result.After.Fields["keep"]
		assert.True(t, hasKeep, "unmapped field should remain")
	})

	t.Run("static fields", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			StaticFields: map[string]interface{}{
				"source":  "datastream",
				"version": "1.0",
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: 1, Type: "int"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		sourceField, ok := result.After.Fields["source"]
		assert.True(t, ok)
		assert.Equal(t, "datastream", sourceField.Value)

		versionField, ok := result.After.Fields["version"]
		assert.True(t, ok)
		assert.Equal(t, "1.0", versionField.Value)
	})

	t.Run("field converter", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			FieldConverters: map[string]FieldConverter{
				"amount": func(value interface{}) (interface{}, error) {
					// Convert cents to dollars
					if cents, ok := value.(int); ok {
						return float64(cents) / 100.0, nil
					}
					return value, nil
				},
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":     {Name: "id", Value: 1, Type: "int"},
					"amount": {Name: "amount", Value: 1050, Type: "int"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		amountField, ok := result.After.Fields["amount"]
		assert.True(t, ok)
		assert.Equal(t, 10.5, amountField.Value)
	})

	t.Run("converter error keeps original value", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			FieldConverters: map[string]FieldConverter{
				"value": func(value interface{}) (interface{}, error) {
					return nil, errors.New("converter error")
				},
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"value": {Name: "value", Value: "original", Type: "varchar"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		// Should keep original value on error
		valueField, ok := result.After.Fields["value"]
		assert.True(t, ok)
		assert.Equal(t, "original", valueField.Value)
	})

	t.Run("transform both before and after", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			FieldMapping: map[string]string{
				"name": "full_name",
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeUpdate,
			Before: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: 1, Type: "int"},
					"name": {Name: "name", Value: "old_name", Type: "varchar"},
				},
			},
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: 1, Type: "int"},
					"name": {Name: "name", Value: "new_name", Type: "varchar"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		// Check Before was transformed
		_, hasOldBefore := result.Before.Fields["name"]
		_, hasNewBefore := result.Before.Fields["full_name"]
		assert.False(t, hasOldBefore)
		assert.True(t, hasNewBefore)

		// Check After was transformed
		_, hasOldAfter := result.After.Fields["name"]
		_, hasNewAfter := result.After.Fields["full_name"]
		assert.False(t, hasOldAfter)
		assert.True(t, hasNewAfter)
	})

	t.Run("combined mapping, converter and static fields", func(t *testing.T) {
		mt := NewMappingTransformer(&MappingConfig{
			FieldMapping: map[string]string{
				"user_id": "id",
			},
			FieldConverters: map[string]FieldConverter{
				"status": func(value interface{}) (interface{}, error) {
					if v, ok := value.(int); ok {
						if v == 1 {
							return "active", nil
						}
						return "inactive", nil
					}
					return value, nil
				},
			},
			StaticFields: map[string]interface{}{
				"source": "datastream",
			},
		})

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			After: event.RowData{
				Fields: map[string]event.Field{
					"user_id": {Name: "user_id", Value: 123, Type: "int"},
					"status":  {Name: "status", Value: 1, Type: "int"},
				},
			},
		}

		result, err := mt.Transform(e)
		require.NoError(t, err)

		// Check field mapping
		_, hasOldUserID := result.After.Fields["user_id"]
		_, hasNewID := result.After.Fields["id"]
		assert.False(t, hasOldUserID)
		assert.True(t, hasNewID)

		// Check converter
		statusField := result.After.Fields["status"]
		assert.Equal(t, "active", statusField.Value)

		// Check static field
		sourceField := result.After.Fields["source"]
		assert.Equal(t, "datastream", sourceField.Value)
	})
}

func TestMappingTransformerAddMethods(t *testing.T) {
	mt := NewMappingTransformer(nil)

	// Test AddFieldMapping
	mt.AddFieldMapping("old_field", "new_field")
	assert.Equal(t, "new_field", mt.fieldMapping["old_field"])

	// Test AddFieldConverter
	converter := func(value interface{}) (interface{}, error) {
		return value, nil
	}
	mt.AddFieldConverter("test_field", converter)
	_, hasConverter := mt.fieldConverters["test_field"]
	assert.True(t, hasConverter)

	// Test AddStaticField
	mt.AddStaticField("static_key", "static_value")
	assert.Equal(t, "static_value", mt.staticFields["static_key"])

	// Test getter methods
	assert.NotNil(t, mt.FieldMapping())
	assert.NotNil(t, mt.FieldConverters())
	assert.NotNil(t, mt.StaticFields())
}

func TestMappingTransformerWithEmptyRowData(t *testing.T) {
	mt := NewMappingTransformer(&MappingConfig{
		FieldMapping: map[string]string{
			"old": "new",
		},
	})

	e := &event.ChangeEvent{
		Type:  event.EventTypeInsert,
		After: event.RowData{Fields: map[string]event.Field{}},
	}

	result, err := mt.Transform(e)
	require.NoError(t, err)
	assert.Empty(t, result.After.Fields)
}

func TestMappingTransformerWithNilBefore(t *testing.T) {
	mt := NewMappingTransformer(&MappingConfig{
		FieldMapping: map[string]string{
			"name": "full_name",
		},
	})

	e := &event.ChangeEvent{
		Type:   event.EventTypeInsert,
		Before: event.RowData{Fields: map[string]event.Field{}}, // Empty, not nil
		After: event.RowData{
			Fields: map[string]event.Field{
				"name": {Name: "name", Value: "test", Type: "varchar"},
			},
		},
	}

	result, err := mt.Transform(e)
	require.NoError(t, err)

	// Only After should be transformed (Before is empty)
	_, hasBeforeField := result.Before.Fields["full_name"]
	assert.False(t, hasBeforeField)

	_, hasAfterField := result.After.Fields["full_name"]
	assert.True(t, hasAfterField)
}
