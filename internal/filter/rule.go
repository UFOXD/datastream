package filter

import (
	"regexp"

	"github.com/UFOXD/datastream/pkg/event"
)

// RuleFilter filters events based on configurable rules.
// It supports include/exclude patterns for tables and event types.
type RuleFilter struct {
	// Include tables patterns (regex)
	includeTables []*regexp.Regexp

	// Exclude tables patterns (regex)
	excludeTables []*regexp.Regexp

	// Include databases patterns (regex)
	includeDatabases []*regexp.Regexp

	// Exclude databases patterns (regex)
	excludeDatabases []*regexp.Regexp

	// Include event types
	includeTypes map[event.EventType]bool

	// Exclude event types
	excludeTypes map[event.EventType]bool
}

// Config holds the configuration for RuleFilter.
type Config struct {
	// IncludeTables specifies table patterns to include (regex).
	// Format: "database.table" or "*.table" or "database.*"
	IncludeTables []string `json:"includeTables" toml:"includeTables"`

	// ExcludeTables specifies table patterns to exclude (regex).
	ExcludeTables []string `json:"excludeTables" toml:"excludeTables"`

	// IncludeDatabases specifies database patterns to include (regex).
	IncludeDatabases []string `json:"includeDatabases" toml:"includeDatabases"`

	// ExcludeDatabases specifies database patterns to exclude (regex).
	ExcludeDatabases []string `json:"excludeDatabases" toml:"excludeDatabases"`

	// IncludeTypes specifies event types to include.
	IncludeTypes []event.EventType `json:"includeTypes" toml:"includeTypes"`

	// ExcludeTypes specifies event types to exclude.
	ExcludeTypes []event.EventType `json:"excludeTypes" toml:"excludeTypes"`
}

// NewRuleFilter creates a new rule-based filter from the configuration.
func NewRuleFilter(cfg *Config) (*RuleFilter, error) {
	rf := &RuleFilter{
		includeTypes: make(map[event.EventType]bool),
		excludeTypes: make(map[event.EventType]bool),
	}

	// Compile include table patterns
	for _, pattern := range cfg.IncludeTables {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		rf.includeTables = append(rf.includeTables, re)
	}

	// Compile exclude table patterns
	for _, pattern := range cfg.ExcludeTables {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		rf.excludeTables = append(rf.excludeTables, re)
	}

	// Compile include database patterns
	for _, pattern := range cfg.IncludeDatabases {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		rf.includeDatabases = append(rf.includeDatabases, re)
	}

	// Compile exclude database patterns
	for _, pattern := range cfg.ExcludeDatabases {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		rf.excludeDatabases = append(rf.excludeDatabases, re)
	}

	// Set event type filters
	for _, t := range cfg.IncludeTypes {
		rf.includeTypes[t] = true
	}
	for _, t := range cfg.ExcludeTypes {
		rf.excludeTypes[t] = true
	}

	return rf, nil
}

// Filter implements the Filter interface.
func (rf *RuleFilter) Filter(e *event.ChangeEvent) (bool, error) {
	// 1. Check event type exclusion
	if len(rf.excludeTypes) > 0 && rf.excludeTypes[e.Type] {
		return false, nil
	}

	// 2. Check event type inclusion
	if len(rf.includeTypes) > 0 && !rf.includeTypes[e.Type] {
		return false, nil
	}

	// 3. Check database exclusion
	if len(rf.excludeDatabases) > 0 {
		for _, re := range rf.excludeDatabases {
			if re.MatchString(e.Table.Database) {
				return false, nil
			}
		}
	}

	// 4. Check database inclusion
	if len(rf.includeDatabases) > 0 {
		matched := false
		for _, re := range rf.includeDatabases {
			if re.MatchString(e.Table.Database) {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}

	// 5. Build full table name
	tableName := e.Table.Database + "." + e.Table.Table

	// 6. Check table exclusion
	if len(rf.excludeTables) > 0 {
		for _, re := range rf.excludeTables {
			if re.MatchString(tableName) {
				return false, nil
			}
		}
	}

	// 7. Check table inclusion
	if len(rf.includeTables) > 0 {
		matched := false
		for _, re := range rf.includeTables {
			if re.MatchString(tableName) {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}

	return true, nil
}

// AddIncludeTable adds a table inclusion pattern.
func (rf *RuleFilter) AddIncludeTable(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	rf.includeTables = append(rf.includeTables, re)
	return nil
}

// AddExcludeTable adds a table exclusion pattern.
func (rf *RuleFilter) AddExcludeTable(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	rf.excludeTables = append(rf.excludeTables, re)
	return nil
}

// AddIncludeType adds an event type to include.
func (rf *RuleFilter) AddIncludeType(t event.EventType) {
	rf.includeTypes[t] = true
}

// AddExcludeType adds an event type to exclude.
func (rf *RuleFilter) AddExcludeType(t event.EventType) {
	rf.excludeTypes[t] = true
}
