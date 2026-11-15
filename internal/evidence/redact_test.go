package evidence

import (
	"strings"
	"testing"
)

func TestRedactText(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		notContains []string
		contains    string
	}{
		{
			name:        "jwt",
			in:          "eyJhbGciOiJSUzI1NiIsImtpZCI6Inh5eiJ9.eyJpc3MiOiJrdWJlcm5ldGVzIn0.SflKxwRJSMeKKF2QT4fWpA",
			notContains: []string{"eyJhbGciOiJSUzI1NiIsImtpZCI6Inh5eiJ9"},
			contains:    "[REDACTED_JWT]",
		},
		{
			name:        "jwt with key prefix",
			in:          "token: eyJhbGciOiJSUzI1NiIsImtpZCI6Inh5eiJ9.eyJpc3MiOiJrdWJlcm5ldGVzIn0.SflKxwRJSMeKKF2QT4fWpA",
			notContains: []string{"eyJhbGciOiJSUzI1NiIsImtpZCI6Inh5eiJ9"},
			contains:    "token=[REDACTED]",
		},
		{
			name:        "bearer scheme",
			in:          "credentials: Bearer 0123456789abcdef0123456789abcdef",
			notContains: []string{"0123456789abcdef0123456789abcdef"},
			contains:    "Bearer [REDACTED]",
		},
		{
			name:        "authorization header",
			in:          "Authorization: Bearer 0123456789abcdef0123456789abcdef",
			notContains: []string{"0123456789abcdef0123456789abcdef"},
			contains:    "Authorization: [REDACTED]",
		},
		{
			name:        "basic scheme",
			in:          "auth=Basic dXNlcm5hbWU6cGFzc3dvcmQxMjM0",
			notContains: []string{"dXNlcm5hbWU6cGFzc3dvcmQxMjM0"},
			contains:    "Basic [REDACTED]",
		},
		{
			name:        "password key value",
			in:          `{"password":"s3cr3t-value","retries":3}`,
			notContains: []string{"s3cr3t-value"},
			contains:    "password=[REDACTED]",
		},
		{
			name:        "api key key value",
			in:          "api_key = abc123def456ghi789",
			notContains: []string{"abc123def456ghi789"},
			contains:    "api_key=[REDACTED]",
		},
		{
			name:        "aws access key",
			in:          "using AKIAIOSFODNN7EXAMPLE for s3",
			notContains: []string{"AKIAIOSFODNN7EXAMPLE"},
			contains:    "[REDACTED_AWS_KEY]",
		},
		{
			name:        "private key block",
			in:          "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAK\n-----END RSA PRIVATE KEY-----",
			notContains: []string{"MIIEowIBAAK"},
			contains:    "[REDACTED_PRIVATE_KEY]",
		},
		{
			name:        "plain text untouched",
			in:          "container app exited with code 1 after 3 restarts",
			notContains: nil,
			contains:    "container app exited with code 1 after 3 restarts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, changed := RedactText(tt.in)
			for _, s := range tt.notContains {
				if strings.Contains(out, s) {
					t.Errorf("output still contains %q: %q", s, out)
				}
			}
			if !strings.Contains(out, tt.contains) {
				t.Errorf("output missing %q: %q", tt.contains, out)
			}
			wantChanged := len(tt.notContains) > 0
			if changed != wantChanged {
				t.Errorf("changed = %v, want %v", changed, wantChanged)
			}
		})
	}
}

func TestSensitiveKeyName(t *testing.T) {
	sensitive := []string{
		"PASSWORD", "db_password", "SECRET_KEY", "API_TOKEN",
		"api_key", "AWS_SECRET_ACCESS_KEY", "client-secret", "PRIVATE_KEY", "AUTH_TOKEN",
	}
	for _, name := range sensitive {
		if !SensitiveKeyName(name) {
			t.Errorf("SensitiveKeyName(%q) = false, want true", name)
		}
	}
	plain := []string{"APP_MODE", "PORT", "LOG_LEVEL", "GOMAXPROCS"}
	for _, name := range plain {
		if SensitiveKeyName(name) {
			t.Errorf("SensitiveKeyName(%q) = true, want false", name)
		}
	}
}
