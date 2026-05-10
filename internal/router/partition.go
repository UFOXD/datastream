package router

import (
	"fmt"
	"math/rand"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/utils"
)

// PartitionStrategy defines the partition selection strategy.
type PartitionStrategy string

const (
	// PartitionByTable routes by table name.
	PartitionByTable PartitionStrategy = "table"
	// PartitionByPK routes by primary key values.
	PartitionByPK PartitionStrategy = "pk"
	// PartitionByField routes by specified field values.
	PartitionByField PartitionStrategy = "field"
	// PartitionRandom routes randomly.
	PartitionRandom PartitionStrategy = "random"
)

// PartitionRouter routes events to partitions (for Kafka, etc.).
type PartitionRouter struct {
	// Partition strategy
	strategy PartitionStrategy

	// Number of partitions
	partitionCount int

	// Partition key fields
	partitionKey []string
}

// NewPartitionRouter creates a new partition-based router.
func NewPartitionRouter(cfg *RouterConfig) *PartitionRouter {
	return &PartitionRouter{
		strategy:       cfg.PartitionStrategy,
		partitionCount: cfg.PartitionCount,
		partitionKey:   cfg.PartitionKey,
	}
}

// Route implements the Router interface.
// Returns the partition number as a string.
func (pr *PartitionRouter) Route(e *event.ChangeEvent) (string, error) {
	var key string

	switch pr.strategy {
	case PartitionByTable:
		key = e.Table.Database + "." + e.Table.Table

	case PartitionByPK:
		// Use primary key values
		for _, pk := range e.Table.PrimaryKeyColumns {
			if field, ok := e.After.GetField(pk); ok {
				key += fmt.Sprintf("%v|", field.Value)
			}
		}

	case PartitionByField:
		// Use specified fields
		for _, field := range pr.partitionKey {
			if value, ok := e.After.Get(field); ok {
				key += fmt.Sprintf("%v|", value)
			}
		}

	case PartitionRandom:
		return fmt.Sprintf("%d", rand.Intn(pr.partitionCount)), nil
	}

	// Hash to partition
	partition := utils.FNV32(key) % uint32(pr.partitionCount)
	return fmt.Sprintf("%d", partition), nil
}
