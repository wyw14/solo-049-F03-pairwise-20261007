package pass

import (
	"errors"
	"fmt"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/scheduler"
)

// Canceler releases every resource a pass holds once its phase flips to
// canceled. Each release is scoped to the pass's own identifiers, and an
// already-canceled pass short-circuits before any release runs, so a
// repeated cancel never frees resources a later task has acquired.
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
		return value, nil
	}
	if err := c.passes.Cancel(passID, reason); err != nil {
		return Pass{}, err
	}
	value, _ = c.passes.Get(passID)
	cause := "pass canceled"
	if reason != "" {
		cause = "pass canceled: " + reason
	}
	var errs []error
	reservationReleased := false
	if value.ReservationID != "" {
		if _, err := c.timeline.Release(value.ReservationID); err != nil {
			errs = append(errs, fmt.Errorf("release reservation: %w", err))
		} else {
			reservationReleased = true
		}
	}
	ownershipReleased := false
	if state, ok := c.fleet.State(value.AntennaID); ok && state.OwnerPassID == passID {
		if err := c.fleet.Release(value.AntennaID, passID, state.OwnerGeneration); err != nil {
			errs = append(errs, fmt.Errorf("release antenna: %w", err))
		} else {
			ownershipReleased = true
		}
	}
	retired, err := c.commands.RetireForPass(passID, cause)
	if err != nil {
		errs = append(errs, fmt.Errorf("retire pointing commands: %w", err))
	}
	attemptsAborted, err := c.attempts.AbortForPass(passID, cause)
	if err != nil {
		errs = append(errs, fmt.Errorf("abort receiver attempts: %w", err))
	}
	recordingsAborted, err := c.recorder.AbortForPass(passID, cause)
	if err != nil {
		errs = append(errs, fmt.Errorf("abort recordings: %w", err))
	}
	_, _ = c.audit.Record(audit.Event{Component: "pass", Action: "canceled", Subject: passID, Fields: map[string]any{
		"reason": reason, "reservation_released": reservationReleased, "ownership_released": ownershipReleased,
		"commands_retired": retired, "attempts_aborted": attemptsAborted, "recordings_aborted": recordingsAborted,
	}})
	return value, errors.Join(errs...)
}
