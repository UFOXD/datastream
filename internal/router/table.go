package router

import (
	"github.com/UFOXD/datastream/pkg/event"
)

// TableRouter routes events based on table name.
type TableRouter struct {
	// Table name -> Sink ID mapping
	tableMapping map[string]string

	// Default Sink ID
	defaultSink string
}

// NewTableRouter creates a new table-based router.
func NewTableRouter(cfg *RouterConfig) *TableRouter {
	return &TableRouter{
		tableMapping: cfg.TableMapping,
		defaultSink:  cfg.DefaultSink,
	}
}

// Route implements the Router interface.
// Routes events based on the table name.
func (tr *TableRouter) Route(e *event.ChangeEvent) (string, error) {
	tableName := e.Table.Database + "." + e.Table.Table

	if sinkID, ok := tr.tableMapping[tableName]; ok {
		return sinkID, nil
	}

	return tr.defaultSink, nil
}
