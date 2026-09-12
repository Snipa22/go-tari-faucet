package faucet

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
)

func newTestHandler(repo *fakeRepo, wallet *fakeWallet, clock *fakeClock) *Handler {
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  clock,
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour},
	}
	return NewHandler(svc)
}

func TestHandler_Index_RendersForm(t *testing.T) {
	h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{now: time.Now()})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.Index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<form") {
		t.Fatal("expected the index page to contain a <form> element")
	}
}

func TestHandler_Request_RejectsMalformedAddressWith400(t *testing.T) {
	h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{now: time.Now()})
	form := url.Values{"address": {"not-a-real-address"}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.Request(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "doesn&#39;t look like a valid Tari address") && !strings.Contains(rec.Body.String(), "valid Tari address") {
		t.Fatalf("expected an address-validation error message, got body: %s", rec.Body.String())
	}
}

func TestHandler_Request_SuccessRendersConfirmation(t *testing.T) {
	valid := validTestnetAddress(t)
	wallet := &fakeWallet{sendResp: successResponse(7)}
	h := newTestHandler(&fakeRepo{}, wallet, &fakeClock{now: time.Now()})
	form := url.Values{"address": {valid}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.Request(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Success") {
		t.Fatalf("expected a success message, got body: %s", rec.Body.String())
	}
	if len(wallet.sentRecipients) != 1 {
		t.Fatalf("expected exactly one wallet call, got %d", len(wallet.sentRecipients))
	}
}

func TestHandler_Request_RateLimitedReturns429(t *testing.T) {
	valid := validTestnetAddress(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := &fakeRepo{lastByKey: map[string]time.Time{
		"addr:" + valid: base,
	}}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	clock := &fakeClock{now: base.Add(time.Minute)}
	h := newTestHandler(repo, wallet, clock)

	form := url.Values{"address": {valid}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.RemoteAddr = "192.0.2.55:1234"
	rec := httptest.NewRecorder()

	h.Request(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429, body: %s", rec.Code, rec.Body.String())
	}
	if len(wallet.sentRecipients) != 0 {
		t.Fatal("wallet should not be called for a rate-limited request")
	}
}

func TestHandler_Request_UsesXForwardedForForRateLimitKey(t *testing.T) {
	addrOne := validTestnetAddress(t)
	addrTwo := validTestnetAddress(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	clock := &fakeClock{now: base}
	h := newTestHandler(repo, wallet, clock)

	// First request behind a proxy, real client is 198.51.100.9.
	form := url.Values{"address": {addrOne}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-For", "198.51.100.9")
	req.RemoteAddr = "10.0.0.1:1234" // the reverse proxy's own address
	rec := httptest.NewRecorder()
	h.Request(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}

	// Second request, different address, same real client IP via XFF but a
	// *different* proxy RemoteAddr -- must still be rate limited on IP.
	clock.now = base.Add(time.Minute)
	form2 := url.Values{"address": {addrTwo}}
	req2 := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form2.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req2.Header.Set("X-Forwarded-For", "198.51.100.9")
	req2.RemoteAddr = "10.0.0.2:5678"
	rec2 := httptest.NewRecorder()
	h.Request(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429, body: %s", rec2.Code, rec2.Body.String())
	}
}

func TestHandler_Healthz(t *testing.T) {
	t.Run("200 when healthy", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{}, &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
			Status: tari_generated.CheckConnectivityResponse_Online,
		}}, &fakeClock{now: time.Now()})
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("503 when postgres is down", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{pingErr: errStub}, &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
			Status: tari_generated.CheckConnectivityResponse_Online,
		}}, &fakeClock{now: time.Now()})
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})
}

// errStub is a trivial sentinel error for tests that only care that an
// error occurred, not its content.
var errStub = &stubError{"stub error"}

type stubError struct{ msg string }

func (e *stubError) Error() string { return e.msg }

func TestHandler_Request_MethodNotAllowed(t *testing.T) {
	h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{now: time.Now()})
	req := httptest.NewRequest(http.MethodGet, "/request", nil)
	rec := httptest.NewRecorder()
	h.Request(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandler_Request_AmountReflectedInSuccessMessage(t *testing.T) {
	valid := validTestnetAddress(t)
	wallet := &fakeWallet{sendResp: successResponse(99)}
	svc := &Service{
		Repo:   &fakeRepo{},
		Wallet: wallet,
		Clock:  &fakeClock{now: time.Now()},
		Config: Config{DispenseAmount: 2500000, RateLimitWindow: time.Hour},
	}
	h := NewHandler(svc)
	form := url.Values{"address": {valid}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.Request(rec, req)

	if !strings.Contains(rec.Body.String(), strconv.FormatUint(2500000, 10)) {
		t.Fatalf("expected the dispense amount in the response body, got: %s", rec.Body.String())
	}
}
