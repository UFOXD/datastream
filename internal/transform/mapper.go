package transform

import (
	"github.com/UFOXD/datastream/pkg/event"
)

// FieldConverter is a function that converts a field value.
type FieldConverter func(value interface{}) (interface{}, error)

// MappingConfig holds the configuration for MappingTransformer.
type MappingConfig struct {
	// FieldMapping maps source field names to target field names.
	FieldMapping map[string]string `json:"fieldMapping" toml:"fieldMapping"`

	// FieldConverters maps field names to converter functions.
	// These are set programmatically, not via config.
	FieldConverters map[string]FieldConverter `json:"-" toml:"-"`

	// StaticFields are added to all events.
	StaticFields map[string]interface{} `json:"staticFields" toml:"staticFields"`
}

// MappingTransformer transforms events by mapping fields and adding static fields.
type MappingTransformer struct {
	fieldMapping    map[string]string
	fieldConverters map[string]FieldConverter
	staticFields    map[string]interface{}
}

// NewMappingTransformer creates a new mapping transformer.
func NewMappingTransformer(cfg *MappingConfig) *MappingTransformer {
	mt := &MappingTransformer{
		fieldMapping:    make(map[string]string),
		fieldConverters: make(map[string]FieldConverter),
		staticFields:    make(map[string]interface{}),
	}

	if cfg != nil {
		// Copy field mappings
		for src, dst := range cfg.FieldMapping {
			mt.fieldMapping[src] = dst
		}

		// Copy field converters
		for name, converter := range cfg.FieldConverters {
			mt.fieldConverters[name] = converter
		}

		// Copy static fields
		for k, v := range cfg.StaticFields {
			mt.staticFields[k] = v
		}
	}

	return mt
}

// Transform implements the Transformer interface.
func (mt *MappingTransformer) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error) {
	// 1. Apply field mapping and converters to After data
	if len(mt.fieldMapping) > 0 || len(mt.fieldConverters) > 0 {
		e.After = *mt.transformRowData(&e.After)
	}

	// 2. Apply field mapping and converters to Before data (if present)
	if len(e.Before.Fields) > 0 && (len(mt.fieldMapping) > 0 || len(mt.fieldConverters) > 0) {
		e.Before = *mt.transformRowData(&e.Before)
	}

	// 3. Add static fields to After data
	for name, value := range mt.staticFields {
		e.After.Fields[name] = event.Field{
			Name:  name,
			Value: value,
			Type:  "static",
		}
	}

	return e, nil
}

// transformRowData applies field mapping and converters to a RowData.
func (mt *MappingTransformer) transformRowData(row *event.RowData) *event.RowData {
	if row == nil || len(row.Fields) == 0 {
		return row
	}

	result := event.NewRowData()

	for name, field := range row.Fields {
		value := field.Value

		// Apply field converter if exists
		if converter, ok := mt.fieldConverters[name]; ok {
			converted, err := converter(value)
			if err != nil {
				// On error, keep original value
				result.Fields[name] = field
				continue
			}
			value = converted
		}

		// Apply field mapping if exists
		targetName := name
		if mapped, ok := mt.fieldMapping[name]; ok {
			targetName = mapped
		}

		// Create the new field with potentially new name and value
		result.Fields[targetName] = event.Field{
			Name:  targetName,
			Value: value,
			Type:  field.Type,
			Null:  field.Null,
		}
	}

	return result
}

// AddFieldMapping adds a field mapping.
func (mt *MappingTransformer) AddFieldMapping(src, dst string) {
	mt.fieldMapping[src] = dst
}

// AddFieldConverter adds a field converter.
func (mt *MappingTransformer) AddFieldConverter(name string, converter FieldConverter) {
	mt.fieldConverters[name] = converter
}

// AddStaticField adds a static field.
func (mt *MappingTransformer) AddStaticField(name string, value interface{}) {
	mt.staticFields[name] = value
}

// FieldMapping returns the current field mapping.
func (mt *MappingTransformer) FieldMapping() map[string]string {
	return mt.fieldMapping
}

// FieldConverters returns the current field converters.
func (mt *MappingTransformer) FieldConverters() map[string]FieldConverter {
	return mt.fieldConverters
}

// StaticFields returns the current static fields.
func (mt *MappingTransformer) StaticFields() map[string]interface{} {
	return mt.staticFields
}
