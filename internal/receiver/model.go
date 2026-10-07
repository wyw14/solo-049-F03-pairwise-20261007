package receiver

import "time"

type AttemptStatus string

const (
	AttemptPending AttemptStatus = "pending"
	AttemptLocked  AttemptStatus = "locked"
	AttemptFailed  AttemptStatus = "failed"
	AttemptAborted AttemptStatus = "aborted"
)

type Attempt struct {
	ID         string        `json:"id"`
	PassID     string        `json:"pass_id"`
	ChainID    string        `json:"chain_id"`
	Number     int64         `json:"number"`
	Status     AttemptStatus `json:"status"`
	Epoch      int64         `json:"epoch"`
	ErrorCode  string        `json:"error_code,omitempty"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at,omitempty"`
}

type AcquisitionResult struct {
	PassID    string
	AttemptID string
	ChainID   string
	Locked    bool
	Synced    bool
	Epoch     int64
	ErrorCode string
}

type ResultSink interface {
	ApplyAcquisitionResult(AcquisitionResult) error
}
