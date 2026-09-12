package faucet

import "html/template"

// indexTemplate renders the faucet's single page: a plain server-rendered
// HTML form (no JS framework, no build step, matching this ecosystem's
// Go-templates convention) plus an optional status message from a prior
// submission.
var indexTemplate = template.Must(template.New("index").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Tari Testnet Faucet</title>
  <style>
    body { font-family: sans-serif; max-width: 640px; margin: 4rem auto; padding: 0 1rem; }
    input[type=text] { width: 100%; padding: 0.5rem; font-family: monospace; }
    button { padding: 0.5rem 1.5rem; margin-top: 0.75rem; }
    .message { padding: 0.75rem; margin-bottom: 1rem; border-radius: 4px; }
    .message.error { background: #fdecea; color: #611a15; }
    .message.success { background: #eaf6ea; color: #1e4620; }
  </style>
</head>
<body>
  <h1>Tari Testnet Faucet</h1>
  <p>Enter a testnet Tari address below to receive a small amount of test Tari.</p>
  {{if .Message}}
  <div class="message {{if .IsError}}error{{else}}success{{end}}">{{.Message}}</div>
  {{end}}
  <form method="post" action="/request">
    <label for="address">Tari address</label>
    <input type="text" id="address" name="address" placeholder="12abc...|network|features|..." value="{{.Address}}" required>
    <br>
    <button type="submit">Request test Tari</button>
  </form>
</body>
</html>
`))

// indexData is the template context for indexTemplate.
type indexData struct {
	Address string
	Message string
	IsError bool

	// StatusCode is the HTTP status to send with the rendered page. Zero
	// means "use the default" (200, or 400 if IsError).
	StatusCode int
}
