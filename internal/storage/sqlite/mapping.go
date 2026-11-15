package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

func nowMillis() int64 { return time.Now().UTC().UnixMilli() }

func toMillis(t time.Time) int64 { return t.UTC().UnixMilli() }

func fromMillis(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func nullMillis(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: toMillis(*t), Valid: true}
}

func timeFromNull(ms sql.NullInt64) *time.Time {
	if !ms.Valid {
		return nil
	}
	t := fromMillis(ms.Int64)
	return &t
}

// isUniqueViolation reports whether the error is a SQLite constraint
// violation on a unique index.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// isForeignKeyViolation reports whether the error is a SQLite foreign
// key constraint violation.
func isForeignKeyViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

// wrapErr adds operation context and maps sentinel errors.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%s: %w", op, storage.ErrNotFound)
	case isUniqueViolation(err):
		return fmt.Errorf("%s: %w: %v", op, storage.ErrAlreadyExists, err)
	case isForeignKeyViolation(err):
		return fmt.Errorf("%s: referenced record is missing: %w", op, err)
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
