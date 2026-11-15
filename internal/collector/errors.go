package collector

import (
	"context"
	"errors"
	"net"
	"syscall"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Error classifications used in warnings and collection runs.
const (
	ErrKindNotFound     = "not_found"
	ErrKindForbidden    = "forbidden"
	ErrKindUnauthorized = "unauthorized"
	ErrKindThrottled    = "throttled"
	ErrKindTimeout      = "timeout"
	ErrKindCanceled     = "canceled"
	ErrKindTransient    = "transient"
	ErrKindUnknown      = "unknown"
)

// ClassifyError maps a collection error to a stable classification so
// callers can decide between retrying and giving up.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ErrKindTimeout
	case errors.Is(err, context.Canceled):
		return ErrKindCanceled
	case apierrors.IsNotFound(err):
		return ErrKindNotFound
	case apierrors.IsForbidden(err):
		return ErrKindForbidden
	case apierrors.IsUnauthorized(err):
		return ErrKindUnauthorized
	case apierrors.IsTooManyRequests(err):
		return ErrKindThrottled
	case apierrors.IsServiceUnavailable(err), apierrors.IsInternalError(err), apierrors.IsServerTimeout(err):
		return ErrKindTransient
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrKindTimeout
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return ErrKindTransient
	}
	return ErrKindUnknown
}
