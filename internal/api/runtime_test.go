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

func TestRouterRejectsOverlappingReservation(t *testing.T) {
	runtime := newTestRuntime(t)
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

func TestCancelReleasesPassResourcesIdempotently(t *testing.T) {
	runtime := newTestRuntime(t)
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
	follow, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: value.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: value.Window.LOS.Add(10 * time.Minute), LOS: value.Window.LOS.Add(18 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(follow.ID, follow.Window); err != nil {
		t.Fatal(err)
	}
	followReservation, err := runtime.Reservations.ReservePass(follow.ID, follow.AntennaID)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.SetReservation(follow.ID, followReservation.ReservationID, followReservation.Generation); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Replanner.QueueInitial(follow.ID); err != nil {
		t.Fatal(err)
	}

	canceled, err := runtime.Canceler.Cancel(value.ID, "operator request")
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Phase != passsvc.Canceled || canceled.FailureReason != "operator request" {
		t.Fatalf("unexpected cancel state: %#v", canceled)
	}
	for _, interval := range runtime.Timeline.List(value.AntennaID) {
		if interval.PassID == value.ID && interval.Status != scheduler.Released {
			t.Fatalf("canceled pass reservation still %s", interval.Status)
		}
		if interval.PassID == follow.ID && interval.Status == scheduler.Released {
			t.Fatal("cancel released the follow-up reservation")
		}
	}
	state, _ := runtime.Antennas.State(value.AntennaID)
	if state.OwnerPassID != "" {
		t.Fatalf("antenna still owned by %s", state.OwnerPassID)
	}
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired {
			t.Fatal("pointing command of canceled pass left executable")
		}
	}
	for _, command := range runtime.Commands.List(follow.ID) {
		if command.Retired {
			t.Fatal("cancel retired a follow-up pointing command")
		}
	}
	if _, found := runtime.Attempts.Current(value.ID); found {
		t.Fatal("canceled pass still has a current receive attempt")
	}
	for _, item := range runtime.Attempts.List() {
		if item.ID == attempt.ID && item.Status != receiver.AttemptAborted {
			t.Fatalf("receive attempt is %s, not aborted", item.Status)
		}
	}
	for _, item := range runtime.Recorder.List() {
		if item.ID == segment.ID {
			if item.Status != recorder.Aborted {
				t.Fatalf("recording is %s, not aborted", item.Status)
			}
			if !strings.Contains(item.SealReason, "operator request") {
				t.Fatalf("abort reason %q does not name the cancel reason", item.SealReason)
			}
		}
	}

	if _, err := runtime.Antennas.Claim(followReservation); err != nil {
		t.Fatal(err)
	}
	again, err := runtime.Canceler.Cancel(value.ID, "operator request")
	if err != nil {
		t.Fatal(err)
	}
	if again.Phase != passsvc.Canceled {
		t.Fatalf("repeated cancel changed phase to %s", again.Phase)
	}
	state, _ = runtime.Antennas.State(value.AntennaID)
	if state.OwnerPassID != follow.ID {
		t.Fatalf("repeated cancel released antenna now owned by %s", state.OwnerPassID)
	}
	for _, interval := range runtime.Timeline.List(value.AntennaID) {
		if interval.PassID == follow.ID && interval.Status == scheduler.Released {
			t.Fatal("repeated cancel released the follow-up reservation")
		}
	}
	for _, command := range runtime.Commands.List(follow.ID) {
		if command.Retired {
			t.Fatal("repeated cancel retired a follow-up pointing command")
		}
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
