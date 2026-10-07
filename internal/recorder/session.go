package recorder

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type session struct {
	segment Segment
	buffer  []Frame
}

type Manager struct {
	mu       sync.RWMutex
	path     string
	sessions map[string]*session
	writer   *FrameWriter
	audit    *audit.Trail
}

func NewManager(paths store.Paths, trail *audit.Trail) (*Manager, error) {
	manager := &Manager{
		path: paths.Snapshot("recording-sessions"), sessions: make(map[string]*session),
		writer: NewFrameWriter(paths), audit: trail,
	}
	var segments []Segment
	found, err := store.ReadJSON(manager.path, &segments)
	if err != nil {
		return nil, err
	}
	if found {
		for _, segment := range segments {
			copy := segment
			manager.sessions[segment.ID] = &session{segment: copy}
		}
	}
	return manager, nil
}

func (m *Manager) Start(passID, ownerStation string, generation int64) (Segment, error) {
	if passID == "" || ownerStation == "" || generation < 1 {
		return Segment{}, fmt.Errorf("pass, recorder owner and generation are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, current := range m.sessions {
		if current.segment.PassID == passID && current.segment.Status != Sealed && current.segment.Status != Aborted {
			return Segment{}, fmt.Errorf("pass %s already has an active recorder", passID)
		}
	}
	segment := Segment{
		ID: uuid.NewString(), PassID: passID, OwnerStation: ownerStation,
		OwnerGeneration: generation, Status: Open, StartedAt: time.Now().UTC(),
	}
	m.sessions[segment.ID] = &session{segment: segment, buffer: make([]Frame, 0, 256)}
	if err := m.flushLocked(); err != nil {
		delete(m.sessions, segment.ID)
		return Segment{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "started", Subject: segment.ID, Fields: map[string]any{"pass_id": passID, "owner": ownerStation, "generation": generation}})
	return segment, nil
}

func (m *Manager) Buffer(segmentID string, frame Frame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.sessions[segmentID]
	if !found {
		return fmt.Errorf("recording segment %s not found", segmentID)
	}
	if current.segment.Status != Open {
		return fmt.Errorf("recording segment %s is not accepting frames", segmentID)
	}
	if frame.Received.IsZero() {
		frame.Received = time.Now().UTC()
	}
	current.buffer = append(current.buffer, frame)
	current.segment.FramesBuffered = len(current.buffer)
	return nil
}

func (m *Manager) ActiveForPass(passID string) (Segment, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, current := range m.sessions {
		if current.segment.PassID == passID && current.segment.Status != Sealed && current.segment.Status != Aborted {
			return current.segment, true
		}
	}
	return Segment{}, false
}

func (m *Manager) List() []Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]Segment, 0, len(m.sessions))
	for _, current := range m.sessions {
		values = append(values, current.segment)
	}
	return values
}

func (m *Manager) StopForFailure(passID, attemptID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, current := range m.sessions {
		if current.segment.PassID == passID && current.segment.Status == Open {
			current.segment.Status = Aborted
			current.segment.SealReason = reason + " (attempt " + attemptID + ")"
			current.segment.SealedAt = time.Now().UTC()
			return m.flushLocked()
		}
	}
	return nil
}

// AbortForPass keeps the segments a pass has already produced but marks them
// aborted with the reason, instead of draining and sealing them.
func (m *Manager) AbortForPass(passID, reason string) error {
	m.mu.Lock()
	var aborted []Segment
	for _, current := range m.sessions {
		if current.segment.PassID != passID {
			continue
		}
		if current.segment.Status != Open && current.segment.Status != Draining {
			continue
		}
		current.segment.Status = Aborted
		current.segment.SealReason = reason
		current.segment.SealedAt = time.Now().UTC()
		aborted = append(aborted, current.segment)
	}
	var err error
	if len(aborted) > 0 {
		err = m.flushLocked()
	}
	m.mu.Unlock()
	if err != nil {
		return err
	}
	for _, segment := range aborted {
		_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "aborted", Subject: segment.ID, Fields: map[string]any{"pass_id": passID, "reason": reason}})
	}
	return nil
}

func (m *Manager) flushLocked() error {
	values := make([]Segment, 0, len(m.sessions))
	for _, current := range m.sessions {
		values = append(values, current.segment)
	}
	return store.WriteJSON(m.path, values)
}
