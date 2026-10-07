package receiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type AttemptDispatcher struct {
	mu       sync.RWMutex
	path     string
	attempts map[string]Attempt
	current  map[string]string
	sink     ResultSink
	audit    *audit.Trail
}

func NewAttemptDispatcher(paths store.Paths, trail *audit.Trail) (*AttemptDispatcher, error) {
	dispatcher := &AttemptDispatcher{
		path: paths.Snapshot("receiver-attempts"), attempts: make(map[string]Attempt),
		current: make(map[string]string), audit: trail,
	}
	var attempts []Attempt
	found, err := store.ReadJSON(dispatcher.path, &attempts)
	if err != nil {
		return nil, err
	}
	if found {
		for _, attempt := range attempts {
			dispatcher.attempts[attempt.ID] = attempt
			if current, ok := dispatcher.attempts[dispatcher.current[attempt.PassID]]; !ok || attempt.Number > current.Number {
				dispatcher.current[attempt.PassID] = attempt.ID
			}
		}
	}
	return dispatcher, nil
}

func (d *AttemptDispatcher) SetSink(sink ResultSink) {
	d.mu.Lock()
	d.sink = sink
	d.mu.Unlock()
}

func (d *AttemptDispatcher) Begin(passID, chainID string) (Attempt, error) {
	if passID == "" || chainID == "" {
		return Attempt{}, fmt.Errorf("pass and receiver chain are required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	number := int64(1)
	if previousID := d.current[passID]; previousID != "" {
		number = d.attempts[previousID].Number + 1
	}
	attempt := Attempt{ID: uuid.NewString(), PassID: passID, ChainID: chainID, Number: number, Status: AttemptPending, StartedAt: time.Now().UTC()}
	d.attempts[attempt.ID] = attempt
	d.current[passID] = attempt.ID
	if err := d.flushLocked(); err != nil {
		return Attempt{}, err
	}
	_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "attempt.started", Subject: attempt.ID, Fields: map[string]any{"pass_id": passID, "number": number}})
	return attempt, nil
}

func (d *AttemptDispatcher) Deliver(result AcquisitionResult) error {
	d.mu.Lock()
	attempt, found := d.attempts[result.AttemptID]
	if !found || attempt.PassID != result.PassID {
		d.mu.Unlock()
		return fmt.Errorf("receiver attempt does not match result")
	}
	current := d.current[result.PassID]
	if current != result.AttemptID {
		_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "result.stale", Subject: result.AttemptID, Severity: audit.Warning, Fields: map[string]any{"current_attempt": current, "error_code": result.ErrorCode}})
		d.mu.Unlock()
		return nil
	}
	if attempt.Status != AttemptPending {
		_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "result.late", Subject: result.AttemptID, Severity: audit.Warning, Fields: map[string]any{"status": string(attempt.Status), "error_code": result.ErrorCode}})
		d.mu.Unlock()
		return nil
	}
	if result.Locked && result.Synced {
		attempt.Status = AttemptLocked
		attempt.Epoch = result.Epoch
	} else {
		attempt.Status = AttemptFailed
		attempt.ErrorCode = result.ErrorCode
	}
	attempt.FinishedAt = time.Now().UTC()
	d.attempts[attempt.ID] = attempt
	sink := d.sink
	err := d.flushLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if sink == nil {
		return fmt.Errorf("receiver result sink is not configured")
	}
	return sink.ApplyAcquisitionResult(result)
}

// AbortForPass terminates every pending attempt of one pass. Finished
// attempts are historical records and stay untouched, so a repeated abort
// cannot disturb attempts a later task may have started.
func (d *AttemptDispatcher) AbortForPass(passID, reason string) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	aborted := 0
	for id, attempt := range d.attempts {
		if attempt.PassID != passID || attempt.Status != AttemptPending {
			continue
		}
		attempt.Status = AttemptAborted
		attempt.ErrorCode = reason
		attempt.FinishedAt = time.Now().UTC()
		d.attempts[id] = attempt
		aborted++
	}
	if aborted == 0 {
		return 0, nil
	}
	if err := d.flushLocked(); err != nil {
		return 0, err
	}
	_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "attempts.aborted", Subject: passID, Fields: map[string]any{"count": aborted, "reason": reason}})
	return aborted, nil
}

func (d *AttemptDispatcher) Current(passID string) (Attempt, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id := d.current[passID]
	attempt, found := d.attempts[id]
	return attempt, found
}

func (d *AttemptDispatcher) List() []Attempt {
	d.mu.RLock()
	defer d.mu.RUnlock()
	values := make([]Attempt, 0, len(d.attempts))
	for _, attempt := range d.attempts {
		values = append(values, attempt)
	}
	return values
}

func (d *AttemptDispatcher) flushLocked() error {
	values := make([]Attempt, 0, len(d.attempts))
	for _, attempt := range d.attempts {
		values = append(values, attempt)
	}
	return store.WriteJSON(d.path, values)
}
