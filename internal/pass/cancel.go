package pass

import (
	"fmt"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// Canceler releases every resource a pass holds when an operator cancels it.
type Canceler struct {
	passes   *Service
	timeline *scheduler.Timeline
	fleet    *antenna.Fleet
	commands *scheduler.CommandQueue
	attempts *receiver.AttemptDispatcher
	recorder *recorder.Manager
	audit    *audit.Trail
}

func NewCanceler(passes *Service, timeline *scheduler.Timeline, fleet *antenna.Fleet, commands *scheduler.CommandQueue, attempts *receiver.AttemptDispatcher, manager *recorder.Manager, trail *audit.Trail) *Canceler {
	return &Canceler{passes: passes, timeline: timeline, fleet: fleet, commands: commands, attempts: attempts, recorder: manager, audit: trail}
}

func (c *Canceler) Cancel(passID, reason string) (Pass, error) {
	value, found := c.passes.Get(passID)
	if !found {
		return Pass{}, fmt.Errorf("pass %s not found", passID)
	}
	if value.Phase == Canceled {
		// A repeated cancel is a no-op: everything this pass held was released
		// the first time, and the antenna may belong to another pass by now.
		return value, nil
	}
	if err := c.passes.Cancel(passID, reason); err != nil {
		return Pass{}, err
	}
	cause := "pass canceled"
	if reason != "" {
		cause += ": " + reason
	}
	var firstErr error
	fail := func(err error) {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	reservationReleased := false
	if value.ReservationID != "" {
		for _, interval := range c.timeline.List("") {
			if interval.ReservationID != value.ReservationID || interval.PassID != passID || interval.Status == scheduler.Released {
				continue
			}
			if _, err := c.timeline.Release(interval.ReservationID); err != nil {
				fail(err)
			} else {
				reservationReleased = true
			}
		}
	}
	ownershipReleased := false
	if state, ok := c.fleet.State(value.AntennaID); ok && state.OwnerPassID == passID {
		if err := c.fleet.Release(value.AntennaID, passID, state.OwnerGeneration); err != nil {
			fail(err)
		} else {
			ownershipReleased = true
		}
	}
	retired, err := c.commands.RetireForPass(passID, cause)
	fail(err)
	aborted, err := c.attempts.AbortForPass(passID, cause)
	fail(err)
	fail(c.recorder.AbortForPass(passID, cause))
	_, _ = c.audit.Record(audit.Event{Component: "pass", Action: "cancel.released", Subject: passID, Fields: map[string]any{
		"reason": reason, "reservation_released": reservationReleased, "ownership_released": ownershipReleased,
		"commands_retired": retired, "attempts_aborted": aborted,
	}})
	updated, _ := c.passes.Get(passID)
	return updated, firstErr
}
