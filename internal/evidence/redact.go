package evidence

import "regexp"

// redactRule replaces one credential-like pattern with a marker.
type redactRule struct {
	re          *regexp.Regexp
	replacement string
}

var redactRules = []redactRule{
	// Key-value credentials in logs and config. Runs first so later
	// rules do not re-redact its replacement markers.
	{regexp.MustCompile(`(?i)\b(password|passwd|secret|token|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|client[_-]?secret|private[_-]?key|session[_-]?token)\b["']?\s*[:=]\s*["']?[^\s"',;&]+`), "${1}=[REDACTED]"},
	// Bearer and Basic authorization schemes.
	{regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/=-]{12,}`), "Bearer [REDACTED]"},
	{regexp.MustCompile(`(?i)\bbasic\s+[a-z0-9+/=]{12,}`), "Basic [REDACTED]"},
	// JSON web tokens.
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}\b`), "[REDACTED_JWT]"},
	// Authorization header values (redacted to end of line).
	{regexp.MustCompile(`(?i)(authorization["']?\s*[:=]\s*)[^\r\n]+`), "${1}[REDACTED]"},
	// AWS access key IDs.
	{regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`), "[REDACTED_AWS_KEY]"},
	// PEM private key blocks.
	{regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`), "[REDACTED_PRIVATE_KEY]"},
}

// RedactText replaces credential-like patterns. It reports whether any
// replacement was made.
func RedactText(text string) (string, bool) {
	out := text
	changed := false
	for _, r := range redactRules {
		if r.re.MatchString(out) {
			out = r.re.ReplaceAllString(out, r.replacement)
			changed = true
		}
	}
	return out, changed
}

// sensitiveNameRe matches env var or annotation names that commonly
// carry credentials.
var sensitiveNameRe = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|apikey|credential|private[_-]?key|auth)`)

// SensitiveKeyName reports whether a name looks credential-bearing.
func SensitiveKeyName(name string) bool {
	return sensitiveNameRe.MatchString(name)
}
