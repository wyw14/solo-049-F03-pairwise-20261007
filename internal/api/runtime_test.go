package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/rf"
	"orbitlink/internal/satellite"
	"orbitlink/internal/scheduler"
	"orbitlink/internal/weather"
)

func TestOperationsPagesAndAPIsUseRuntimeState(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	for _, target := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/healthz", "/api/passes", "/api/antennas", "/api/rf-chains", "/api/recordings"} {
		response, err := server.Client().Get(server.URL + target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d", target, response.StatusCode)
		}
		_ = response.Body.Close()
	}
}

func TestPassRevisionAcquisitionAndRecordingFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	decision, err := runtime.Passes.AcceptDiversity(value.ID, results)
	if err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	session, err := runtime.NewDecoderSession(value.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}
	if _, err := runtime.Recorder.Drain(context.Background(), active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Recorder.Seal(active.ID, "integration complete"); err != nil {
		t.Fatal(err)
	}
}

func TestEphemerisClockAndWindSafetyFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	previous, _ := runtime.Ephemerides.Current(value.SatelliteID)
	if _, err := runtime.Ephemerides.Import(satellite.Ephemeris{SatelliteID: value.SatelliteID, Revision: previous.Revision + 1, Epoch: time.Now().UTC(), Line1: "revised orbital element one", Line2: "revised orbital element two"}); err != nil {
		t.Fatal(err)
	}
	commands := runtime.Commands.List(value.ID)
	if len(commands) == 0 || commands[len(commands)-1].Revision != previous.Revision+1 {
		t.Fatal("revised pointing commands were not queued")
	}
	if _, err := runtime.Clock.Apply(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 2 * time.Second, Source: "gps", Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.Clock.Corrections()) != 1 {
		t.Fatal("clock correction was not persisted")
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	if err != nil || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("wind safety did not converge: %#v %v", result, err)
	}
}

func TestCancelReleasesPassResources(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	value := runtime.Passes.List()[0]
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	segment, err := runtime.Recorder.Start(value.ID, value.OwnerStation, value.OwnerGeneration)
	if err != nil {
		t.Fatal(err)
	}
	response := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/passes/"+value.ID+"/cancel", `{"reason":"operator request"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cancel returned %d", response.StatusCode)
	}
	var canceled passsvc.Pass
	decodeBody(t, response, &canceled)
	if canceled.Phase != passsvc.Canceled || canceled.FailureReason != "operator request" {
		t.Fatalf("unexpected pass after cancel: %#v", canceled)
	}
	released := false
	for _, interval := range runtime.Timeline.List(value.AntennaID) {
		if interval.ReservationID == value.ReservationID {
			released = interval.Status == scheduler.Released
		}
	}
	if !released {
		t.Fatal("cancel did not release the antenna timeline reservation")
	}
	state, _ := runtime.Antennas.State(value.AntennaID)
	if state.OwnerPassID != "" || state.OwnerGeneration != 0 {
		t.Fatalf("cancel did not release antenna ownership: %#v", state)
	}
	commands := runtime.Commands.List(value.ID)
	if len(commands) == 0 {
		t.Fatal("expected seeded pointing commands")
	}
	for _, command := range commands {
		if !command.Retired || command.RetireCause != "pass canceled: operator request" {
			t.Fatalf("cancel did not retire pointing command: %#v", command)
		}
	}
	if due := runtime.Commands.Due(time.Now().Add(time.Hour)); len(due) != 0 {
		t.Fatalf("retired commands still due: %#v", due)
	}
	aborted, found := findAttempt(runtime, attempt.ID)
	if !found || aborted.Status != receiver.AttemptAborted {
		t.Fatalf("cancel did not abort the receive attempt: %#v", aborted)
	}
	recordings := runtime.Recorder.List()
	var recording recorder.Segment
	for _, item := range recordings {
		if item.ID == segment.ID {
			recording = item
		}
	}
	if recording.Status != recorder.Aborted || recording.SealReason != "pass canceled: operator request" {
		t.Fatalf("recording was not kept as aborted with reason: %#v", recording)
	}
	late := receiver.AcquisitionResult{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 9}
	if err := runtime.Attempts.Deliver(late); err != nil {
		t.Fatal(err)
	}
	after, _ := runtime.Passes.Get(value.ID)
	if after.Phase != passsvc.Canceled {
		t.Fatalf("late acquisition result revived a canceled pass: %#v", after)
	}
	if current, _ := findAttempt(runtime, attempt.ID); current.Status != receiver.AttemptAborted {
		t.Fatalf("late result mutated an aborted attempt: %#v", current)
	}
}

func TestRepeatedCancelDoesNotReleaseOtherPass(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	if _, err := runtime.Canceler.Cancel(first.ID, "operator request"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: now.Add(20 * time.Minute), LOS: now.Add(28 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	reservation, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID)
	if err != nil {
		t.Fatalf("canceled pass did not free the timeline: %v", err)
	}
	if err := runtime.Passes.SetReservation(second.ID, reservation.ReservationID, reservation.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Antennas.Claim(reservation); err != nil {
		t.Fatalf("canceled pass did not free antenna ownership: %v", err)
	}
	if err := runtime.Replanner.QueueInitial(second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Canceler.Cancel(first.ID, "operator request"); err != nil {
		t.Fatal(err)
	}
	for _, interval := range runtime.Timeline.List(first.AntennaID) {
		if interval.ReservationID == reservation.ReservationID && interval.Status == scheduler.Released {
			t.Fatal("repeated cancel released the next pass's reservation")
		}
	}
	state, _ := runtime.Antennas.State(first.AntennaID)
	if state.OwnerPassID != second.ID {
		t.Fatalf("repeated cancel released the next pass's ownership: %#v", state)
	}
	for _, command := range runtime.Commands.List(second.ID) {
		if command.Retired {
			t.Fatalf("repeated cancel retired the next pass's command: %#v", command)
		}
	}
}

func findAttempt(runtime *Runtime, id string) (receiver.Attempt, bool) {
	for _, attempt := range runtime.Attempts.List() {
		if attempt.ID == id {
			return attempt, true
		}
	}
	return receiver.Attempt{}, false
}

func TestRouterRejectsOverlappingReservation(t *testing.T) {	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.AOS.Add(time.Minute), LOS: first.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err == nil {
		t.Fatal("overlapping reservation unexpectedly succeeded")
	}
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func decodeBody(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, target, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
