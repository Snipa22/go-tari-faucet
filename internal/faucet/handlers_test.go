package faucet

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
)

// newTestHandler builds a Handler with an empty StatusCache -- tests that
// care about the cached balance/connectivity values seed one directly via
// newTestHandlerWithCache instead.
func newTestHandler(repo *fakeRepo, wallet *fakeWallet, clock *fakeClock) *Handler {
	return newTestHandlerWithCache(repo, wallet, clock, NewStatusCache())
}

// newTestHandlerWithCache is like newTestHandler but lets the caller
// inject a pre-populated StatusCache, so tests can exercise Index/Healthz
// reading a fake cached balance/connectivity state directly instead of
// relying on a live per-request wallet call.
func newTestHandlerWithCache(repo *fakeRepo, wallet *fakeWallet, clock *fakeClock, cache *StatusCache) *Handler {
	svc := &Service{
		Repo:   repo,
		Wallet: wallet,
		Clock:  clock,
		Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour, Ticker: "tXTM", NetworkLabel: "Testnet"},
	}
	return NewHandler(svc, cache)
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

func TestHandler_Index_RendersWalletBalance(t *testing.T) {
	wallet := &fakeWallet{balanceResp: &tari_generated.GetBalanceResponse{
		AvailableBalance: 1234567,
	}}
	cache := NewStatusCache()
	cache.poll(wallet) // seed the cache directly, as StartPolling would in the background
	h := newTestHandlerWithCache(&fakeRepo{}, wallet, &fakeClock{now: time.Now()}, cache)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.Index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "1.234567 XTM") {
		t.Fatalf("expected the formatted XTM balance in the response body, got: %s", rec.Body.String())
	}
}

func TestHandler_Index_RendersGracefullyWhenBalanceLookupFails(t *testing.T) {
	wallet := &fakeWallet{balanceErr: errors.New("grpc: unavailable")}
	cache := NewStatusCache()
	cache.poll(wallet) // seed the cache directly with the failed poll result
	h := newTestHandlerWithCache(&fakeRepo{}, wallet, &fakeClock{now: time.Now()}, cache)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.Index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<form") {
		t.Fatal("expected the index page to still contain a <form> element when the balance lookup fails")
	}
	if !strings.Contains(rec.Body.String(), "balance unavailable") {
		t.Fatalf("expected a graceful 'balance unavailable' message, got: %s", rec.Body.String())
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
		wallet := &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
			Status: tari_generated.CheckConnectivityResponse_Online,
		}}
		cache := NewStatusCache()
		cache.poll(wallet)
		h := newTestHandlerWithCache(&fakeRepo{}, wallet, &fakeClock{now: time.Now()}, cache)
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("503 when postgres is down", func(t *testing.T) {
		wallet := &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
			Status: tari_generated.CheckConnectivityResponse_Online,
		}}
		cache := NewStatusCache()
		cache.poll(wallet)
		h := newTestHandlerWithCache(&fakeRepo{pingErr: errStub}, wallet, &fakeClock{now: time.Now()}, cache)
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})

	t.Run("503 when the last wallet connectivity poll was not online", func(t *testing.T) {
		wallet := &fakeWallet{connectivity: &tari_generated.CheckConnectivityResponse{
			Status: tari_generated.CheckConnectivityResponse_Offline,
		}}
		cache := NewStatusCache()
		cache.poll(wallet)
		h := newTestHandlerWithCache(&fakeRepo{}, wallet, &fakeClock{now: time.Now()}, cache)
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503, body: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("503 when the cache has never successfully polled", func(t *testing.T) {
		h := newTestHandlerWithCache(&fakeRepo{}, &fakeWallet{}, &fakeClock{now: time.Now()}, NewStatusCache())
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()
		h.Healthz(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503, body: %s", rec.Code, rec.Body.String())
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
	h := NewHandler(svc, NewStatusCache())
	form := url.Values{"address": {valid}}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.Request(rec, req)

	if !strings.Contains(rec.Body.String(), "2.5 XTM") {
		t.Fatalf("expected the XTM-formatted dispense amount in the response body, got: %s", rec.Body.String())
	}
}

func TestHandler_Request_HoneypotPopulated_RejectedWithoutReachingWalletOrRateLimit(t *testing.T) {
	valid := validTestnetAddress(t)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	h := newTestHandler(repo, wallet, &fakeClock{now: time.Now()})

	form := url.Values{
		"address":         {valid},
		honeypotFieldName: {"http://spam.example/"},
	}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.Request(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), genericRejectionMessage("tXTM")) {
		t.Fatalf("expected the generic rejection message, got: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "honeypot") {
		t.Fatal("the rendered page must not reveal that a honeypot was tripped")
	}
	if len(wallet.sentRecipients) != 0 {
		t.Fatal("wallet.SendTransactions must not be called when the honeypot field is populated")
	}
	if repo.lookupCalls != 0 {
		t.Fatal("the rate-limit lookup must not be reached when the honeypot field is populated")
	}
	if len(repo.recorded) != 0 {
		t.Fatal("no audit row should be recorded for a honeypot-rejected request")
	}
}

func TestHandler_Request_EmptyHoneypot_ProceedsNormally(t *testing.T) {
	valid := validTestnetAddress(t)
	repo := &fakeRepo{}
	wallet := &fakeWallet{sendResp: successResponse(1)}
	h := newTestHandler(repo, wallet, &fakeClock{now: time.Now()})

	form := url.Values{
		"address":         {valid},
		honeypotFieldName: {""},
	}
	req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.Request(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body: %s", rec.Code, rec.Body.String())
	}
	if len(wallet.sentRecipients) != 1 {
		t.Fatalf("expected exactly one wallet call, got %d", len(wallet.sentRecipients))
	}
	if repo.lookupCalls != 1 {
		t.Fatalf("expected exactly one rate-limit lookup, got %d", repo.lookupCalls)
	}
}

// TestHandler_Index_TickerBrandingIsConfigurable covers that the index
// page's branding text (title, h1, intro paragraph, submit button) is
// driven entirely by Config.Ticker, not a hardcoded "tXTM"/"Tari" -- both
// the testnet ("tXTM") and mainnet ("XTM") ticker strings must render
// correctly from the same binary. NetworkLabel is held fixed at
// "Testnet" here (its own configurability is covered separately by
// TestHandler_Index_NetworkLabelIsConfigurable) so this test isolates
// Ticker's effect.
func TestHandler_Index_TickerBrandingIsConfigurable(t *testing.T) {
	for _, ticker := range []string{"tXTM", "XTM"} {
		t.Run(ticker, func(t *testing.T) {
			svc := &Service{
				Repo:   &fakeRepo{},
				Wallet: &fakeWallet{},
				Clock:  &fakeClock{now: time.Now()},
				Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour, Ticker: ticker, NetworkLabel: "Testnet"},
			}
			h := NewHandler(svc, NewStatusCache())
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			h.Index(rec, req)

			body := rec.Body.String()
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200, body: %s", rec.Code, body)
			}
			for _, want := range []string{
				"<title>Testnet " + ticker + " Faucet</title>",
				"<h1>Testnet " + ticker + " Faucet</h1>",
				"receive a small amount of " + ticker + ".",
				"<button type=\"submit\">Request " + ticker + "</button>",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("expected body to contain %q, got: %s", want, body)
				}
			}
			// "Tari address" wording refers to the address format, not
			// the network ticker, and must remain untouched regardless
			// of Config.Ticker.
			if !strings.Contains(body, "Tari address") {
				t.Fatalf("expected the 'Tari address' label/placeholder to remain untouched, got: %s", body)
			}
		})
	}
}

// TestHandler_Index_NetworkLabelIsConfigurable covers that the index
// page's title/h1/intro-paragraph network label is driven by
// Config.NetworkLabel rather than a hardcoded "Testnet", and that the
// default zero-value-equivalent config ("Testnet"/"tXTM") renders
// byte-identically to the pre-NetworkLabel wording, while a mainnet
// config ("Mainnet"/"XTM") renders correctly too -- proving the same
// binary can serve either without a hardcoded testnet/mainnet switch.
func TestHandler_Index_NetworkLabelIsConfigurable(t *testing.T) {
	tests := []struct {
		networkLabel   string
		ticker         string
		wantTitle      string
		wantH1         string
		wantIntroLabel string // the lowercased label as it appears in the intro paragraph
	}{
		{networkLabel: "Testnet", ticker: "tXTM", wantTitle: "Testnet tXTM Faucet", wantH1: "Testnet tXTM Faucet", wantIntroLabel: "testnet"},
		{networkLabel: "Mainnet", ticker: "XTM", wantTitle: "Mainnet XTM Faucet", wantH1: "Mainnet XTM Faucet", wantIntroLabel: "mainnet"},
	}
	for _, tt := range tests {
		t.Run(tt.networkLabel, func(t *testing.T) {
			svc := &Service{
				Repo:   &fakeRepo{},
				Wallet: &fakeWallet{},
				Clock:  &fakeClock{now: time.Now()},
				Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour, Ticker: tt.ticker, NetworkLabel: tt.networkLabel},
			}
			h := NewHandler(svc, NewStatusCache())
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			h.Index(rec, req)

			body := rec.Body.String()
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200, body: %s", rec.Code, body)
			}
			for _, want := range []string{
				"<title>" + tt.wantTitle + "</title>",
				"<h1>" + tt.wantH1 + "</h1>",
				"Enter a " + tt.wantIntroLabel + " Tari address below",
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("expected body to contain %q, got: %s", want, body)
				}
			}
		})
	}
}

// TestHandler_Request_HoneypotMessage_UsesConfiguredTicker covers that
// the honeypot rejection message is built from Config.Ticker for both
// the testnet and mainnet ticker strings, rather than a hardcoded
// "tXTM"/"test Tari".
func TestHandler_Request_HoneypotMessage_UsesConfiguredTicker(t *testing.T) {
	for _, ticker := range []string{"tXTM", "XTM"} {
		t.Run(ticker, func(t *testing.T) {
			valid := validTestnetAddress(t)
			svc := &Service{
				Repo:   &fakeRepo{},
				Wallet: &fakeWallet{sendResp: successResponse(1)},
				Clock:  &fakeClock{now: time.Now()},
				Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour, Ticker: ticker},
			}
			h := NewHandler(svc, NewStatusCache())

			form := url.Values{
				"address":         {valid},
				honeypotFieldName: {"http://spam.example/"},
			}
			req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()

			h.Request(rec, req)

			if !strings.Contains(rec.Body.String(), genericRejectionMessage(ticker)) {
				t.Fatalf("expected the ticker-specific rejection message, got: %s", rec.Body.String())
			}
		})
	}
}

// TestHandler_Request_RateLimitedMessage_UsesConfiguredTicker covers that
// the rate-limited message is built from Config.Ticker for both the
// testnet and mainnet ticker strings.
func TestHandler_Request_RateLimitedMessage_UsesConfiguredTicker(t *testing.T) {
	for _, ticker := range []string{"tXTM", "XTM"} {
		t.Run(ticker, func(t *testing.T) {
			valid := validTestnetAddress(t)
			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			repo := &fakeRepo{lastByKey: map[string]time.Time{
				"addr:" + valid: base,
			}}
			svc := &Service{
				Repo:   repo,
				Wallet: &fakeWallet{sendResp: successResponse(1)},
				Clock:  &fakeClock{now: base.Add(time.Minute)},
				Config: Config{DispenseAmount: 1000000, RateLimitWindow: time.Hour, Ticker: ticker},
			}
			h := NewHandler(svc, NewStatusCache())

			form := url.Values{"address": {valid}}
			req := httptest.NewRequest(http.MethodPost, "/request", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.RemoteAddr = "192.0.2.55:1234"
			rec := httptest.NewRecorder()

			h.Request(rec, req)

			if rec.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429, body: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "already received "+ticker+" recently") {
				t.Fatalf("expected the ticker-specific rate-limited message, got: %s", rec.Body.String())
			}
		})
	}
}
