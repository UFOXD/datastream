package event

import "fmt"

// SourceInfo describes the source of an event.
type SourceInfo struct {
	// Connector type
	Connector string `json:"connector"` // mysql, postgresql, mongodb, oracle, sqlserver, mariadb

	// Database connection info
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`

	// Cluster info (if applicable)
	ClusterName string `json:"clusterName,omitempty"`
	ServerName  string `json:"serverName,omitempty"`

	// Collection info
	Snapshot bool `json:"snapshot"` // Whether from snapshot
}

// String returns a string representation of the source.
func (s *SourceInfo) String() string {
	return fmt.Sprintf("%s://%s:%d/%s", s.Connector, s.Host, s.Port, s.Database)
}
