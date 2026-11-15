package collector

import (
	"context"
	"errors"
	"net"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// timeoutError implements net.Error with Timeout() true.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// ensure timeoutError satisfies net.Error.
var _ net.Error = timeoutError{}

func TestClassifyError(t *testing.T) {
	gr := schema.GroupResource{Resource: "pods"}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"not found", apierrors.NewNotFound(gr, "p"), ErrKindNotFound},
		{"forbidden", apierrors.NewForbidden(gr, "p", errors.New("rbac")), ErrKindForbidden},
		{"unauthorized", apierrors.NewUnauthorized("no auth"), ErrKindUnauthorized},
		{"throttled", apierrors.NewTooManyRequestsError("slow down"), ErrKindThrottled},
		{"service unavailable", apierrors.NewServiceUnavailable("down"), ErrKindTransient},
		{"internal error", apierrors.NewInternalError(errors.New("boom")), ErrKindTransient},
		{"server timeout", apierrors.NewServerTimeout(gr, "get", 1), ErrKindTransient},
		{"deadline exceeded", context.DeadlineExceeded, ErrKindTimeout},
		{"canceled", context.Canceled, ErrKindCanceled},
		{"net timeout", timeoutError{}, ErrKindTimeout},
		{"unknown", errors.New("something else"), ErrKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.want {
				t.Errorf("ClassifyError() = %q, want %q", got, tt.want)
			}
		})
	}
}

// stubCollector supports a configurable set of failure types.
type stubCollector struct {
	name  string
	types map[string]bool
}

func (s *stubCollector) Name() string { return s.name }

func (s *stubCollector) Supports(signal domain.FailureSignal) bool {
	return s.types["*"] || s.types[signal.Type]
}

func (s *stubCollector) Collect(context.Context, Request) (Result, error) {
	return Result{}, nil
}

func TestRegistryForSignal(t *testing.T) {
	pod := &stubCollector{name: "pod", types: map[string]bool{"*": true}}
	logs := &stubCollector{name: "logs", types: map[string]bool{"CrashLoopBackOff": true}}
	events := &stubCollector{name: "events", types: map[string]bool{"*": true}}

	r := NewRegistry(pod, logs, events)
	got := r.ForSignal(domain.FailureSignal{Type: "CrashLoopBackOff"})
	if len(got) != 3 {
		t.Errorf("ForSignal() = %v, want [pod logs events]", names(got))
	}

	got = r.ForSignal(domain.FailureSignal{Type: "FailedScheduling"})
	if len(got) != 2 || got[0].Name() != "pod" || got[1].Name() != "events" {
		t.Errorf("ForSignal() = %v, want [pod events]", names(got))
	}
}

func names(cs []Collector) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.Name())
	}
	return out
}
