package store

import (
	"fmt"
	"sync"
	"time"
)

type WindowRecord struct {
	PassID   string    `json:"pass_id"`
	AOS      time.Time `json:"aos"`
	LOS      time.Time `json:"los"`
	Revision int64     `json:"revision"`
	Updated  time.Time `json:"updated"`
}

// WindowStore publishes whole revision-bound windows under one lock.
type WindowStore struct {
	mu      sync.RWMutex
	path    string
	windows map[string]WindowRecord
}

func NewWindowStore(paths Paths) (*WindowStore, error) {
	store := &WindowStore{
		path:    paths.Snapshot("pass-windows"),
		windows: make(map[string]WindowRecord),
	}
	var values []WindowRecord
	found, err := ReadJSON(store.path, &values)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range values {
			store.windows[value.PassID] = value
		}
	}
	return store, nil
}

func (s *WindowStore) Save(window WindowRecord) error {
	if window.PassID == "" || window.Revision < 1 || !window.LOS.After(window.AOS) {
		return fmt.Errorf("invalid revision-bound pass window")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	window.Updated = time.Now().UTC()
	s.windows[window.PassID] = window
	return s.flushLocked()
}

func (s *WindowStore) Get(passID string) (WindowRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	window, found := s.windows[passID]
	return window, found
}

func (s *WindowStore) List() []WindowRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]WindowRecord, 0, len(s.windows))
	for _, window := range s.windows {
		values = append(values, window)
	}
	return values
}

func (s *WindowStore) flushLocked() error {
	values := make([]WindowRecord, 0, len(s.windows))
	for _, window := range s.windows {
		values = append(values, window)
	}
	return WriteJSON(s.path, values)
}
