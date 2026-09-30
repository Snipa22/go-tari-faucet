package faucet

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// fakeHTTPDoer is an in-memory httpDoer test double: it never opens a
// real socket (no httptest.NewServer, no real network call to
// challenges.cloudflare.com), just returns whatever canned
// response/error the test configures.
type fakeHTTPDoer struct {
	resp *http.Response
	err  error
}

func (f *fakeHTTPDoer) Do(_ *http.Request) (*http.Response, error) {
	return f.resp, f.err
}

// jsonResponse builds a minimal *http.Response with body for
// fakeHTTPDoer, mirroring Cloudflare's siteverify contract of always
// returning 200 with a JSON body.
func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestHandler_VerifyTurnstile(t *testing.T) {
	t.Run("success true decodes to (true, nil)", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{})
		h.HTTPClient = &fakeHTTPDoer{resp: jsonResponse(`{"success": true}`)}

		ok, err := h.verifyTurnstile(context.Background(), "secret", "token", "1.2.3.4")
		if err != nil {
			t.Fatalf("verifyTurnstile returned unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("verifyTurnstile = false, want true")
		}
	})

	t.Run("success false decodes to (false, nil) -- a real bot rejection, not an error", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{})
		h.HTTPClient = &fakeHTTPDoer{resp: jsonResponse(`{"success": false}`)}

		ok, err := h.verifyTurnstile(context.Background(), "secret", "token", "1.2.3.4")
		if err != nil {
			t.Fatalf("verifyTurnstile returned unexpected error: %v", err)
		}
		if ok {
			t.Fatal("verifyTurnstile = true, want false")
		}
	})

	t.Run("malformed JSON body fails closed with a non-nil error", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{})
		h.HTTPClient = &fakeHTTPDoer{resp: jsonResponse(`not json`)}

		ok, err := h.verifyTurnstile(context.Background(), "secret", "token", "1.2.3.4")
		if err == nil {
			t.Fatal("verifyTurnstile err = nil, want non-nil on decode failure")
		}
		if ok {
			t.Fatal("verifyTurnstile = true, want false (fail closed) on decode failure")
		}
	})

	t.Run("transport error fails closed with a non-nil error", func(t *testing.T) {
		h := newTestHandler(&fakeRepo{}, &fakeWallet{}, &fakeClock{})
		h.HTTPClient = &fakeHTTPDoer{err: errors.New("network down")}

		ok, err := h.verifyTurnstile(context.Background(), "secret", "token", "1.2.3.4")
		if err == nil {
			t.Fatal("verifyTurnstile err = nil, want non-nil on transport failure")
		}
		if ok {
			t.Fatal("verifyTurnstile = true, want false (fail closed) on transport failure")
		}
	})
}
