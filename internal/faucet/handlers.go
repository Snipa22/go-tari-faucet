package faucet

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Snipa22/go-tari-lib/address"
)

// Handler wires Service into net/http handlers for the faucet's three
// routes: GET / (the form), POST /request (submit an address), and GET
// /healthz (liveness probe).
type Handler struct {
	Service *Service
}

// NewHandler builds a Handler around svc.
func NewHandler(svc *Service) *Handler {
	return &Handler{Service: svc}
}

// Routes registers this Handler's routes on mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/", h.Index)
	mux.HandleFunc("/request", h.Request)
	mux.HandleFunc("/healthz", h.Healthz)
}

// Index renders the plain HTML request form.
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	renderIndex(w, indexData{})
}

// Request handles the form submission: validates the address, applies the
// rate limit, and dispenses test Tari on success. Every outcome -- malformed
// address, rate limited, or a wallet/DB error -- is rendered back into the
// same form with a clear message instead of a 500.
func (h *Handler) Request(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		renderIndex(w, indexData{Message: "Could not parse form submission.", IsError: true})
		return
	}
	rawAddress := r.FormValue("address")
	ip := ClientIP(r)

	result := h.Service.Dispense(r.Context(), rawAddress, ip)
	switch result.Outcome {
	case OutcomeSuccess:
		renderIndex(w, indexData{
			Message: fmt.Sprintf("Success! Sent %d microMinotari (tx id %d).", h.Service.Config.DispenseAmount, result.TxID),
		})
	case OutcomeInvalidAddress:
		renderIndex(w, indexData{
			Address: rawAddress,
			Message: fmt.Sprintf("That doesn't look like a valid Tari address: %v", describeAddressError(result.Err)),
			IsError: true,
		})
	case OutcomeRateLimited:
		renderIndex(w, indexData{
			Address:    rawAddress,
			Message:    fmt.Sprintf("This address or IP has already received test Tari recently. Try again after %s.", result.RetryAfter.Format("2006-01-02 15:04:05 MST")),
			IsError:    true,
			StatusCode: http.StatusTooManyRequests,
		})
	default:
		renderIndex(w, indexData{
			Address: rawAddress,
			Message: "Something went wrong dispensing test Tari. Please try again shortly.",
			IsError: true,
		})
	}
}

// Healthz reports 200 if both Postgres and the wallet GRPC connection are
// reachable, 503 otherwise.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.Service.HealthCheck(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "unhealthy: %v\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "ok")
}

// describeAddressError renders go-tari-lib/address's parse errors as a
// user-facing string; unknown errors fall back to err.Error().
func describeAddressError(err error) string {
	if err == nil {
		return "unknown error"
	}
	if errors.Is(err, address.ErrInvalidAddressString) {
		return "could not recognize the address format"
	}
	return err.Error()
}

func renderIndex(w http.ResponseWriter, data indexData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	switch {
	case data.StatusCode != 0:
		w.WriteHeader(data.StatusCode)
	case data.IsError:
		w.WriteHeader(http.StatusBadRequest)
	}
	_ = indexTemplate.Execute(w, data)
}
