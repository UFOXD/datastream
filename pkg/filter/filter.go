// Package filter provides event filtering capabilities for DataStream.
package filter

import (
	"github.com/UFOXD/datastream/pkg/event"
)

// Filter is the interface for event filters.
// Filters determine whether an event should pass through or be discarded.
type Filter interface {
	// Filter evaluates the event and returns true if it should pass through.
	// Returns false if the event should be filtered out.
	Filter(e *event.ChangeEvent) (bool, error)
}

// FilterChain combines multiple filters into a single filter.
// An event passes through the chain only if all filters return true.
type FilterChain struct {
	filters []Filter
}

// NewFilterChain creates a new filter chain from the given filters.
func NewFilterChain(filters ...Filter) *FilterChain {
	return &FilterChain{filters: filters}
}

// Filter implements the Filter interface.
// Returns true only if all filters in the chain return true.
func (fc *FilterChain) Filter(e *event.ChangeEvent) (bool, error) {
	for _, f := range fc.filters {
		pass, err := f.Filter(e)
		if err != nil {
			return false, err
		}
		if !pass {
			return false, nil
		}
	}
	return true, nil
}

// Add appends a filter to the chain.
func (fc *FilterChain) Add(f Filter) {
	fc.filters = append(fc.filters, f)
}

// Filters returns the list of filters in the chain.
func (fc *FilterChain) Filters() []Filter {
	return fc.filters
}

// PassAllFilter is a filter that passes all events.
type PassAllFilter struct{}

// NewPassAllFilter creates a filter that passes all events.
func NewPassAllFilter() *PassAllFilter {
	return &PassAllFilter{}
}

// Filter always returns true.
func (f *PassAllFilter) Filter(e *event.ChangeEvent) (bool, error) {
	return true, nil
}

// BlockAllFilter is a filter that blocks all events.
type BlockAllFilter struct{}

// NewBlockAllFilter creates a filter that blocks all events.
func NewBlockAllFilter() *BlockAllFilter {
	return &BlockAllFilter{}
}

// Filter always returns false.
func (f *BlockAllFilter) Filter(e *event.ChangeEvent) (bool, error) {
	return false, nil
}
