package faucet

import (
	"fmt"
	"html/template"
	"strconv"
	"strings"
)

// honeypotFieldName is the name of indexTemplate's hidden anti-spam field.
// It's visually hidden off-screen via CSS (position:absolute;
// left:-9999px), not display:none, since real spam bots specifically
// skip display:none fields but fill visually-hidden-via-position fields
// less reliably. A real human never sees or fills it; Handler.Request
// rejects any submission where it's non-empty, silently, as if it were a
// normal validation failure.
const honeypotFieldName = "website"

// indexTemplate renders the faucet's single page: a plain server-rendered
// HTML form (no JS framework, no build step, matching this ecosystem's
// Go-templates convention) plus an optional status message from a prior
// submission and the wallet's current spendable balance. Branding tokens
// (colors/fonts) are reproduced inline from the canonical
// internal/jagtech-branding package -- this is a single Go binary serving
// its own page, so there's no external stylesheet/CDN dependency on that
// repo, just the token values.
var indexTemplate = template.Must(template.New("index").Funcs(template.FuncMap{
	"commas": formatWithCommas,
	"xtm":    formatXTM,
	"lower":  strings.ToLower,
}).Parse(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.NetworkLabel}} {{.Ticker}} Faucet</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@600&family=Inter:wght@300&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet" />
  <style>
    :root {
      --jag-alexandrite-daylight: #1a8a6a;
      --jag-alexandrite-incandescent: #9b2d5a;
      --jag-alexandrite-olive: #6a8a50;
      --jag-deep-teal: #1a5a46;
      --jag-deep-raspberry: #6a1a3a;
      --jag-alexandrite-rose: #c94070;
      --jag-bg-dark: #07080a;
      --jag-text-on-dark: #f0ece4;
      --accent-a: var(--jag-alexandrite-daylight);
      --accent-b: var(--jag-alexandrite-incandescent);
      --accent-grad: linear-gradient(135deg, #1a8a6a 0%, #6a8a50 50%, #9b2d5a 100%);
      --border-glow: rgba(26, 138, 106, 0.3);
    }
    * { box-sizing: border-box; }
    body {
      background: var(--jag-bg-dark);
      color: var(--jag-text-on-dark);
      font-family: 'Inter', sans-serif;
      font-weight: 300;
      max-width: 640px;
      margin: 0 auto;
      padding: 4rem 1.5rem;
    }
    h1, h2, h3 {
      font-family: 'Cormorant Garamond', serif;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.16em;
    }
    h1 { font-size: 1.65rem; margin: 0 0 1.5rem; }
    p { line-height: 1.7; }
    .balance {
      border: 1px solid var(--border-glow);
      border-radius: 4px;
      padding: 1rem 1.25rem;
      margin: 1.75rem 0;
    }
    .balance .label {
      display: block;
      color: var(--jag-alexandrite-daylight);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.75rem;
      font-family: 'JetBrains Mono', monospace;
      margin-bottom: 0.4rem;
    }
    .balance .value {
      font-family: 'JetBrains Mono', monospace;
      font-size: 1.05rem;
    }
    label {
      display: block;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85rem;
      margin-bottom: 0.5rem;
    }
    input[type=text] {
      width: 100%;
      padding: 0.65rem 0.75rem;
      font-family: 'JetBrains Mono', monospace;
      background: transparent;
      border: 1px solid var(--border-glow);
      color: var(--jag-text-on-dark);
      border-radius: 4px;
    }
    input[type=text]:focus { outline: none; border-color: var(--jag-alexandrite-daylight); }
    button {
      padding: 0.65rem 1.75rem;
      margin-top: 1rem;
      background: var(--accent-grad);
      border: none;
      color: var(--jag-bg-dark);
      font-family: 'JetBrains Mono', monospace;
      font-weight: 400;
      letter-spacing: 0.05em;
      border-radius: 4px;
      cursor: pointer;
    }
    .message {
      padding: 0.85rem 1rem;
      margin-bottom: 1.5rem;
      border-radius: 4px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.9rem;
    }
    .message.error {
      background: rgba(106, 26, 58, 0.18);
      color: var(--jag-alexandrite-rose);
      border: 1px solid var(--jag-deep-raspberry);
    }
    .message.success {
      background: rgba(26, 138, 106, 0.12);
      color: var(--jag-alexandrite-daylight);
      border: 1px solid var(--border-glow);
    }
    .hp {
      position: absolute;
      left: -9999px;
      top: auto;
      width: 1px;
      height: 1px;
      overflow: hidden;
    }
    .l2-note {
      margin-top: 2rem;
      padding-top: 1rem;
      border-top: 1px solid var(--border-glow);
      font-size: 0.85rem;
      color: rgba(240, 236, 228, 0.65);
    }
    .l2-note a {
      color: var(--jag-alexandrite-daylight);
    }
  </style>
</head>
<body>
  <h1>{{.NetworkLabel}} {{.Ticker}} Faucet</h1>
  <p>Enter a {{.NetworkLabel | lower}} Tari address below to receive a small amount of {{.Ticker}}.</p>

  <div class="balance">
    <span class="label">Faucet balance</span>
    <span class="value">{{if .FaucetBalanceErr}}balance unavailable{{else}}{{xtm .FaucetBalance}} (spendable){{end}}</span>
  </div>

  {{if .Message}}
  <div class="message {{if .IsError}}error{{else}}success{{end}}">{{.Message}}</div>
  {{end}}
  <form method="post" action="/request">
    <label for="address">Tari address</label>
    <input type="text" id="address" name="address" placeholder="12abc...|network|features|..." value="{{.Address}}" required>
    <div class="hp" aria-hidden="true">
      <label for="website">Leave this field blank</label>
      <input type="text" id="website" name="website" tabindex="-1" autocomplete="off">
    </div>
    <br>
    <button type="submit">Request {{.Ticker}}</button>
  </form>

  <p class="l2-note">Want to use Ootle (L2)? You'll need to burn your {{.Ticker}} first -- see the <a href="https://ootle.tari.com/guides/burn-minotari/" target="_blank" rel="noopener noreferrer">burn guide</a>.</p>
</body>
</html>
`))

// indexData is the template context for indexTemplate.
type indexData struct {
	Address string
	Message string
	IsError bool

	// Ticker is the display word used for the page's branding text
	// (title, h1, intro paragraph, submit button) -- e.g. "tXTM" or
	// "XTM", sourced from Service.Config.Ticker. renderIndex fills this
	// in on every call so callers building indexData literals don't
	// each need to remember to set it.
	Ticker string

	// NetworkLabel is the display word used alongside Ticker in the
	// page's branding text (title, h1, intro paragraph) -- e.g.
	// "Testnet" or "Mainnet", sourced from Service.Config.NetworkLabel.
	// Like Ticker, renderIndex fills this in on every call so callers
	// building indexData literals don't each need to remember to set
	// it.
	NetworkLabel string

	// FaucetBalance is the wallet's current spendable (available)
	// balance in microMinotari. Only meaningful when FaucetBalanceErr is
	// false.
	FaucetBalance uint64
	// FaucetBalanceErr is true when the wallet balance lookup failed --
	// the page still renders (with "balance unavailable") rather than
	// erroring the whole page.
	FaucetBalanceErr bool

	// StatusCode is the HTTP status to send with the rendered page. Zero
	// means "use the default" (200, or 400 if IsError).
	StatusCode int
}

// formatWithCommas renders n with thousands separators (e.g. 1234567 ->
// "1,234,567") for human-readable display of whole-unit amounts. A plain
// manual implementation is fine here -- no need for a full i18n library
// for this.
func formatWithCommas(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var groups []string
	for len(s) > 3 {
		groups = append([]string{s[len(s)-3:]}, groups...)
		s = s[:len(s)-3]
	}
	groups = append([]string{s}, groups...)
	return strings.Join(groups, ",")
}

// microMinotariPerXTM is the atomic-unit precision the wallet GRPC and
// PaymentRecipient work in internally: 1 XTM == 1,000,000 microMinotari.
const microMinotariPerXTM = 1_000_000

// formatXTM renders a raw microMinotari amount (the atomic unit every
// internal code path -- Config.DispenseAmount, wallet calls, DB storage,
// rate-limit math, logging -- continues to use exactly as today) as a
// human-readable "XTM" display string, e.g. 799178469391 ->
// "799,178.469391 XTM", or 100000000 -> "100 XTM" when there's no
// fractional remainder. This is the single shared conversion used by
// every user-facing string that shows an amount -- callers must not
// duplicate the div/mod logic.
//
// Integer division/modulo is used throughout (never floating point) so
// the conversion is exact for a money figure: whole is the number of
// full XTM, frac is the remaining microMinotari, zero-padded to exactly
// 6 digits (the full atomic-unit precision) before any trailing-zero
// trimming.
func formatXTM(n uint64) string {
	whole := n / microMinotariPerXTM
	frac := n % microMinotariPerXTM
	if frac == 0 {
		return formatWithCommas(whole) + " XTM"
	}
	fracStr := strings.TrimRight(fmt.Sprintf("%06d", frac), "0")
	return formatWithCommas(whole) + "." + fracStr + " XTM"
}
