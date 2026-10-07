package api

import (
	"fmt"
	"sync"
	"time"

	"orbitlink/internal/antenna"
	"orbitlink/internal/audit"
	stationclock "orbitlink/internal/clock"
	"orbitlink/internal/console"
	"orbitlink/internal/decoder"
	"orbitlink/internal/handover"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/store"
	"orbitlink/internal/weather"
)

type Runtime struct {
	StateRoot        string
	Paths            store.Paths
	EventLog         *store.AppendLog
	Audit            *audit.Trail
	Catalog          *satellite.Catalog
	Ephemerides      *satellite.EphemerisService
	Clock            *stationclock.Service
	Windows          *store.WindowStore
	Timeline         *scheduler.Timeline
	Commands         *scheduler.CommandQueue
	Reservations     *scheduler.ReservationPlanner
	Antennas         *antenna.Fleet
	Executor         *antenna.Executor
	Stower           *antenna.StowController
	RF               *rf.Controller
	Recorder         *recorder.Manager
	Passes           *passsvc.Service
	WindowRecompute  *passsvc.WindowRecomputer
	Replanner        *passsvc.Replanner
	Extensions       *passsvc.ExtensionService
	Deadlines        *passsvc.DeadlineManager
	Unlocker         *receiver.Unlocker
	Finalizer        *passsvc.Finalizer
	Canceler         *passsvc.Canceler
	Attempts         *receiver.AttemptDispatcher
	RecordingState   *passsvc.RecordingState
	Handovers        *handover.Coordinator
	Weather          *weather.Coordinator
	sessionsMu       sync.Mutex
	receiverSessions map[string]*receiver.Session
}

func NewRuntime(stateRoot string) (*Runtime, error) {
	paths := store.NewPaths(stateRoot)
	if err := paths.Validate(); err != nil {
		return nil, err
	}
	log := store.NewAppendLog(paths)
	trail := audit.NewTrail(log)
	catalog, err := satellite.NewCatalog(paths)
	if err != nil {
		return nil, err
	}
	ephemerides, err := satellite.NewEphemerisService(paths, trail)
	if err != nil {
		return nil, err
	}
	clockService, err := stationclock.NewService(paths, trail)
	if err != nil {
		return nil, err
	}
	windows, err := store.NewWindowStore(paths)
	if err != nil {
		return nil, err
	}
	timeline, err := scheduler.NewTimeline(paths, trail)
	if err != nil {
		return nil, err
	}
	commands, err := scheduler.NewCommandQueue(paths)
	if err != nil {
		return nil, err
	}
	fleet, err := antenna.NewFleet(paths, timeline, trail, []string{"ANT-01", "ANT-02", "ANT-03"})
	if err != nil {
		return nil, err
	}
	rfController, err := rf.NewController(paths, trail, []string{"RF-01", "RF-02", "RF-03"})
	if err != nil {
		return nil, err
	}
	recorderManager, err := recorder.NewManager(paths, trail)
	if err != nil {
		return nil, err
	}
	passes, err := passsvc.NewService(paths, recorderManager, trail)
	if err != nil {
		return nil, err
	}
	windowRecompute := passsvc.NewWindowRecomputer(windows, trail)
	replanner := passsvc.NewReplanner(passes, windowRecompute, commands, trail)
	extensions := passsvc.NewExtensionService(passes, timeline, fleet, trail)
	deadlines := passsvc.NewDeadlineManager(clockService)
	unlocker := receiver.NewUnlocker(trail)
	finalizer := passsvc.NewFinalizer(passes, recorderManager, unlocker, trail, 250*time.Millisecond)
	attempts, err := receiver.NewAttemptDispatcher(paths, trail)
	if err != nil {
		return nil, err
	}
	attempts.SetSink(passes)
	canceler := passsvc.NewCanceler(passes, timeline, fleet, commands, attempts, recorderManager, trail)
	recordingState := passsvc.NewRecordingState(passes, recorderManager)
	handovers := handover.NewCoordinator(recordingState, recorderManager, trail)
	stower := antenna.NewStowController(fleet, trail)
	weatherCoordinator := weather.NewCoordinator(20, rfController, stower, passes, trail)
	runtime := &Runtime{
		StateRoot: stateRoot, Paths: paths, EventLog: log, Audit: trail, Catalog: catalog, Ephemerides: ephemerides,
		Clock: clockService, Windows: windows, Timeline: timeline, Commands: commands,
		Reservations: scheduler.NewReservationPlanner(windows, timeline, 30*time.Second),
		Antennas:     fleet, Executor: antenna.NewExecutor(fleet, commands, trail), Stower: stower,
		RF: rfController, Recorder: recorderManager, Passes: passes, WindowRecompute: windowRecompute,
		Replanner: replanner, Extensions: extensions, Deadlines: deadlines, Unlocker: unlocker,
		Finalizer: finalizer, Canceler: canceler, Attempts: attempts, RecordingState: recordingState,
		Handovers: handovers, Weather: weatherCoordinator, receiverSessions: make(map[string]*receiver.Session),
	}
	ephemerides.Subscribe(replanner.ApplyRevision)
	if err := runtime.seed(); err != nil {
		return nil, err
	}
	return runtime, nil
}

func (r *Runtime) Health() console.Health {
	components := map[string]string{"scheduler": "ready", "antenna": "ready", "rf": "ready", "receiver": "ready", "recorder": "ready", "clock": "ready"}
	now := r.Clock.Now()
	for _, state := range r.Antennas.List() {
		if _, found := r.Timeline.CurrentOwner(state.ID, now); found {
			components["scheduler"] = "active-owner"
		}
	}
	for _, value := range r.Passes.List() {
		if value.Phase == passsvc.Tracking {
			if remaining, err := r.Deadlines.Remaining(value.ID); err == nil && remaining > 0 {
				components["clock"] = "monotonic-active"
			}
		}
	}
	return console.Health{Status: "ok", Service: "orbitlink", Now: r.Clock.Now(), StateRoot: r.StateRoot,
		Components: components,
		Passes:     len(r.Passes.List()), Antennas: len(r.Antennas.List()), RFChains: len(r.RF.List()), Recordings: len(r.Recorder.List())}
}

func (r *Runtime) NewDecoderSession(passID, segmentID string) (*receiver.Session, error) {
	decoderSession, err := decoder.NewSession(passID, segmentID, 64, r.Recorder, r.Audit)
	if err != nil {
		return nil, err
	}
	return receiver.NewSession(passID, decoderSession), nil
}

func (r *Runtime) receiverSession(passID string) (*receiver.Session, error) {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()
	if session := r.receiverSessions[passID]; session != nil {
		return session, nil
	}
	segment, found := r.Recorder.ActiveForPass(passID)
	if !found {
		return nil, fmt.Errorf("pass %s has no active recording", passID)
	}
	session, err := r.NewDecoderSession(passID, segment.ID)
	if err != nil {
		return nil, err
	}
	r.receiverSessions[passID] = session
	return session, nil
}

func (r *Runtime) seed() error {
	if len(r.Catalog.List()) == 0 {
		for _, value := range []satellite.Satellite{{ID: "SAT-A12", Name: "Aquila 12", NORAD: 58112, Band: "S", Enabled: true}, {ID: "SAT-C07", Name: "Cirrus 7", NORAD: 57207, Band: "X", Enabled: true}} {
			if err := r.Catalog.Upsert(value); err != nil {
				return err
			}
			_, err := r.Ephemerides.Import(satellite.Ephemeris{SatelliteID: value.ID, Revision: 1, Epoch: time.Now().UTC(), Line1: "station-managed orbital element line 1", Line2: "station-managed orbital element line 2"})
			if err != nil {
				return err
			}
		}
	}
	if len(r.Passes.List()) == 0 {
		now := r.Clock.Now()
		value, err := r.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-A12", AntennaID: "ANT-01", RFChains: []string{"RF-01", "RF-02"}, RequiredReceiverChains: []string{"RF-01", "RF-02"}, AOS: now.Add(5 * time.Minute), LOS: now.Add(13 * time.Minute), Revision: 1, OwnerStation: "north"})
		if err != nil {
			return err
		}
		if err := r.WindowRecompute.Apply(value.ID, value.Window); err != nil {
			return err
		}
		reservation, err := r.Reservations.ReservePass(value.ID, value.AntennaID)
		if err != nil {
			return err
		}
		if err := r.Passes.SetReservation(value.ID, reservation.ReservationID, reservation.Generation); err != nil {
			return err
		}
		if _, err := r.Antennas.Claim(reservation); err != nil {
			return err
		}
		if err := r.Deadlines.Schedule(value); err != nil {
			return err
		}
		if err := r.Replanner.QueueInitial(value.ID); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) RequirePass(id string) (passsvc.Pass, error) {
	value, found := r.Passes.Get(id)
	if !found {
		return passsvc.Pass{}, fmt.Errorf("pass %s not found", id)
	}
	return value, nil
}
