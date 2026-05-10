// Package transform provides event transformation capabilities for DataStream.
package transform

import (
	"github.com/UFOXD/datastream/pkg/event"
)

// Transformer is the interface for event transformers.
// Transformers modify events as they pass through the pipeline.
type Transformer interface {
	// Transform modifies the event and returns the transformed event.
	// It may return a new event or modify the existing one in place.
	Transform(e *event.ChangeEvent) (*event.ChangeEvent, error)
}

// TransformChain combines multiple transformers into a single transformer.
// Transformers are applied in order.
type TransformChain struct {
	transformers []Transformer
}

// NewTransformChain creates a new transform chain from the given transformers.
func NewTransformChain(transformers ...Transformer) *TransformChain {
	return &TransformChain{transformers: transformers}
}

// Transform implements the Transformer interface.
// Applies all transformers in order.
func (tc *TransformChain) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error) {
	var err error
	for _, t := range tc.transformers {
		e, err = t.Transform(e)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Add appends a transformer to the chain.
func (tc *TransformChain) Add(t Transformer) {
	tc.transformers = append(tc.transformers, t)
}

// Transformers returns the list of transformers in the chain.
func (tc *TransformChain) Transformers() []Transformer {
	return tc.transformers
}

// IdentityTransformer is a transformer that passes events through unchanged.
type IdentityTransformer struct{}

// NewIdentityTransformer creates a transformer that doesn't modify events.
func NewIdentityTransformer() *IdentityTransformer {
	return &IdentityTransformer{}
}

// Transform returns the event unchanged.
func (t *IdentityTransformer) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error) {
	return e, nil
}
