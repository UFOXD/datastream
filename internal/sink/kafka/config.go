// Package kafka provides Kafka sink connector for DataStream.
package kafka

import (
	"github.com/UFOXD/datastream/internal/sink"
)

// Config holds Kafka-specific configuration.
type Config struct {
	// Kafka brokers
	Brokers []string `json:"brokers"`

	// Topic settings
	Topic               string `json:"topic"`
	TopicNamingStrategy string `json:"topicNamingStrategy"` // default, table, database

	// Producer settings
	Acks            string `json:"acks"`            // none, leader, all
	Compression     string `json:"compression"`     // none, gzip, snappy, lz4, zstd
	MaxMessageBytes int    `json:"maxMessageBytes"` // max message size
	QueueBuffer     int    `json:"queueBuffer"`     // queue buffer size
	BatchSize       int    `json:"batchSize"`       // messages per batch
	BatchTimeout    int    `json:"batchTimeout"`    // batch timeout in ms
	Retries         int    `json:"retries"`         // retry count
	RetryBackoff    int    `json:"retryBackoff"`    // retry backoff in ms
	FlushTimeout    int    `json:"flushTimeout"`    // flush timeout in ms

	// Message settings
	KeyFormat    string `json:"keyFormat"`    // avro, json, schema
	ValueFormat  string `json:"valueFormat"`  // avro, json, schema
	PartitionKey string `json:"partitionKey"` // field for partitioning

	// Schema registry (for Avro)
	SchemaRegistryURL string `json:"schemaRegistryUrl"`

	// Security
	SecurityProtocol string `json:"securityProtocol"` // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	SSLCACert        string `json:"sslCaCert"`
	SSLCert          string `json:"sslCert"`
	SSLKey           string `json:"sslKey"`
	SASLMechanism    string `json:"saslMechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUsername     string `json:"saslUsername"`
	SASLPassword     string `json:"saslPassword"`

	// Batch config
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default Kafka configuration.
func DefaultConfig() *Config {
	return &Config{
		TopicNamingStrategy: "default",
		Acks:                "all",
		Compression:         "snappy",
		MaxMessageBytes:     1024 * 1024,
		QueueBuffer:         1000,
		BatchSize:           100,
		BatchTimeout:        10,
		Retries:             3,
		RetryBackoff:        100,
		FlushTimeout:        30000,
		KeyFormat:           "json",
		ValueFormat:         "json",
		PartitionKey:        "id",
		SecurityProtocol:    "PLAINTEXT",
		Batch: sink.BatchConfig{
			Size:    100,
			Timeout: 1000,
			Retries: 3,
		},
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return sink.ErrInvalidConfig
	}
	if c.Topic == "" {
		return sink.ErrInvalidConfig
	}
	return nil
}
