package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
)

func (r *Runtime) listPasses(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Passes.List()})
}

func (r *Runtime) createPass(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		SatelliteID     string    `json:"satellite_id"`
		AntennaID       string    `json:"antenna_id"`
		AOS             time.Time `json:"aos"`
		DurationMinutes int       `json:"duration_minutes"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	if input.DurationMinutes <= 0 {
		input.DurationMinutes = 8
	}
	ephemeris, found := r.Ephemerides.Current(input.SatelliteID)
	if !found {
		writeError(writer, errMessage("satellite has no ephemeris"))
		return
	}
	value, err := r.Passes.Create(passsvc.CreateRequest{SatelliteID: input.SatelliteID, AntennaID: input.AntennaID, RFChains: []string{"RF-01", "RF-02"}, RequiredReceiverChains: []string{"RF-01", "RF-02"}, AOS: input.AOS, LOS: input.AOS.Add(time.Duration(input.DurationMinutes) * time.Minute), Revision: ephemeris.Revision, OwnerStation: "north"})
	if err != nil {
		writeError(writer, err)
		return
	}
	if err := r.WindowRecompute.Apply(value.ID, value.Window); err != nil {
		writeError(writer, err)
		return
	}
	if err := r.Deadlines.Schedule(value); err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, value)
}

func (r *Runtime) reservePass(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "passID")
	value, err := r.RequirePass(id)
	if err != nil {
		writeError(writer, err)
		return
	}
	reservation, err := r.Reservations.ReservePass(id, value.AntennaID)
	if err != nil {
		writeError(writer, err)
		return
	}
	if err := r.Passes.SetReservation(id, reservation.ReservationID, reservation.Generation); err != nil {
		writeError(writer, err)
		return
	}
	if !reservation.Start.After(r.Clock.Now()) {
		if _, err := r.Antennas.Claim(reservation); err != nil {
			writeError(writer, err)
			return
		}
		if _, err := r.Timeline.Activate(reservation.ReservationID); err != nil {
			writeError(writer, err)
			return
		}
	}
	writeJSON(writer, http.StatusOK, reservation)
}

func (r *Runtime) extendPass(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Seconds int64 `json:"seconds"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	value, err := r.Extensions.Extend(chi.URLParam(request, "passID"), time.Duration(input.Seconds)*time.Second)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (r *Runtime) acquirePass(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "passID")
	var input struct {
		ChainID string `json:"chain_id"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	attempt, err := r.Attempts.Begin(id, input.ChainID)
	if err != nil {
		writeError(writer, err)
		return
	}
	if err := r.Passes.BeginAcquisition(id, attempt.ID); err != nil {
		writeError(writer, err)
		return
	}
	value, err := r.RequirePass(id)
	if err != nil {
		writeError(writer, err)
		return
	}
	if _, err := r.Deadlines.Activate(id, time.Until(value.Window.LOS)); err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, attempt)
}

func (r *Runtime) diversityPass(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "passID")
	var input struct {
		Results []struct {
			AttemptID string `json:"attempt_id"`
			ChainID   string `json:"chain_id"`
			ErrorCode string `json:"error_code"`
			Locked    bool   `json:"locked"`
			Synced    bool   `json:"synced"`
			Epoch     int64  `json:"epoch"`
		} `json:"results"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	results := make([]receiver.AcquisitionResult, 0, len(input.Results))
	for _, item := range input.Results {
		results = append(results, receiver.AcquisitionResult{PassID: id, AttemptID: item.AttemptID, ChainID: item.ChainID, Locked: item.Locked, Synced: item.Synced, Epoch: item.Epoch, ErrorCode: item.ErrorCode})
	}
	decision, err := r.Passes.AcceptDiversity(id, results)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, decision)
}

func (r *Runtime) finalizePass(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		ChainID       string `json:"chain_id"`
		UnlockDelayMS int    `json:"unlock_delay_ms"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	if input.UnlockDelayMS > 0 {
		r.Unlocker.SetDelay(input.ChainID, time.Duration(input.UnlockDelayMS)*time.Millisecond)
	}
	result, err := r.Finalizer.Finalize(request.Context(), chi.URLParam(request, "passID"), input.ChainID)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (r *Runtime) deliverAcquisitionResult(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "passID")
	var result receiver.AcquisitionResult
	if err := decodeJSON(request, &result); err != nil {
		writeError(writer, err)
		return
	}
	result.PassID = id
	if err := r.Attempts.Deliver(result); err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"delivered": true})
}

func (r *Runtime) cancelPass(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Reason string `json:"reason"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	value, err := r.Canceler.Cancel(chi.URLParam(request, "passID"), input.Reason)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

type errMessage string

func (e errMessage) Error() string { return string(e) }
