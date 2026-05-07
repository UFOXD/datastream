package event

import "time"

// TransactionInfo holds transaction information.
type TransactionInfo struct {
	// Transaction ID
	ID string `json:"id"`

	// Total events in transaction
	TotalEvents int `json:"totalEvents"`

	// Current event index within transaction
	EventIndex int `json:"eventIndex"`

	// Transaction begin time
	BeginTime time.Time `json:"beginTime"`

	// Transaction commit time
	CommitTime time.Time `json:"commitTime"`
}

// IsBegin returns true if this is the first event in the transaction.
func (t *TransactionInfo) IsBegin() bool {
	return t != nil && t.EventIndex == 0
}

// IsEnd returns true if this is the last event in the transaction.
func (t *TransactionInfo) IsEnd() bool {
	return t != nil && t.EventIndex == t.TotalEvents-1
}

// IsSingleEvent returns true if the transaction has only one event.
func (t *TransactionInfo) IsSingleEvent() bool {
	return t != nil && t.TotalEvents == 1
}
