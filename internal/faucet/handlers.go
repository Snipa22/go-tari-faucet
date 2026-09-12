package faucet

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Snipa22/go-tari-lib/address"
	"github.com/sirupsen/logrus"
)

// genericRejectionMessage is rendered for any request rejection that
// shouldn't reveal its real cause to the caller -- currently just the
// honeypot trip in Request. Kept as a shared constant so the wording
// stays identical to the "something went wrong" default outcome path.
const genericRejectionMessage = "Something went wrong dispensing test Tari. Please try again shortly."

// Handler wires Service into net/http handlers for the faucet's three
// routes: GET / (the form), POST /request (submit an address), and GET
// /healthz (liveness probe).
type Handler struct {
	Service *Service

	// StatusCache holds the last-known wallet balance/connectivity,
	// refreshed in the background (see StatusCache.StartPolling).
	// Index and Healthz read it directly instead of calling
	// Service.Wallet per request, so no HTTP request ever blocks on a
	// live wallet GRPC call. Must be non-nil -- NewHandler always
	// supplies one.
	StatusCache *StatusCache
}

// NewHandler builds a Handler around svc, reading wallet balance/
// connectivity from cache instead of svc's WalletClient directly (see
// StatusCache).
func NewHandler(svc *Service, cache *StatusCache) *Handler {
	return &Handler{Service: svc, StatusCache: cache}
}

// Routes registers this Handler's routes on mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/", h.Index)
	mux.HandleFunc("/request", h.Request)
	mux.HandleFunc("/healthz", h.Healthz)
}

// Index renders the plain HTML request form, including the wallet's
// current spendable balance, read from StatusCache -- an in-memory,
// zero-network-I/O read, never a live wallet GRPC call per request. A
// stale/failed cache entry degrades gracefully -- the form still
// renders, with "balance unavailable" in place of the figure, rather
// than erroring the whole page (same principle Healthz's dependency
// checks already follow).
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data := indexData{}
	balance, balanceOK, _, _ := h.StatusCache.Get()
	if !balanceOK {
		data.FaucetBalanceErr = true
	} else {
		data.FaucetBalance = balance
	}
	renderIndex(w, data)
}

// Request handles the form submission: rejects honeypot-tripped spam,
// validates the address, applies the rate limit, and dispenses test Tari
// on success. Every outcome -- honeypot, malformed address, rate limited,
// or a wallet/DB error -- is rendered back into the same form with a
// clear message instead of a 500.
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

	if honeypot := r.FormValue(honeypotFieldName); honeypot != "" {
		logger := h.Service.Logger
		if logger == nil {
			logger = logrus.StandardLogger()
		}
		logger.WithFields(logrus.Fields{
			"ip": ip,
		}).Info("faucet: honeypot field populated, rejecting as spam")
		renderIndex(w, indexData{
			Address: rawAddress,
			Message: genericRejectionMessage,
			IsError: true,
		})
		return
	}

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
			Message: genericRejectionMessage,
			IsError: true,
		})
	}
}

// Healthz reports 200 if both Postgres and the wallet GRPC connection are
// reachable, 503 otherwise. Postgres is checked live (Repository.Ping)
// on every call -- that's already fast and unaffected by this change.
// The wallet check reads StatusCache's last-known connectivity poll
// instead of dialing the wallet per request, so a slow/hung wallet GRPC
// call can never delay this response.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.Service.Repo.Ping(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "unhealthy: %v\n", err)
		return
	}
	if _, _, walletUp, walletUpOK := h.StatusCache.Get(); !walletUpOK || !walletUp {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, "unhealthy: wallet GRPC connectivity is not online")
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
