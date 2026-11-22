package log

import (
	"fmt"
	"log/slog"
	"strings"
)

// Levels. The four slog levels are re-exported; the others fill in the Google
// Cloud Logging severities that slog has no name for.
const (
	LevelDebug     = slog.LevelDebug // DEBUG
	LevelInfo      = slog.LevelInfo  // INFO
	LevelNotice    = slog.Level(2)   // NOTICE
	LevelWarn      = slog.LevelWarn  // WARNING
	LevelError     = slog.LevelError // ERROR
	LevelCritical  = slog.Level(12)  // CRITICAL
	LevelAlert     = slog.Level(16)  // ALERT
	LevelEmergency = slog.Level(20)  // EMERGENCY
)

// Severity returns the Cloud Logging severity name for l. Levels between two
// named levels map down to the lower one; anything below INFO is DEBUG.
func Severity(l slog.Level) string {
	switch {
	case l < LevelInfo:
		return "DEBUG"
	case l < LevelNotice:
		return "INFO"
	case l < LevelWarn:
		return "NOTICE"
	case l < LevelError:
		return "WARNING"
	case l < LevelCritical:
		return "ERROR"
	case l < LevelAlert:
		return "CRITICAL"
	case l < LevelEmergency:
		return "ALERT"
	default:
		return "EMERGENCY"
	}
}

// ParseLevel parses a level name, case-insensitively: debug, info, notice,
// warn, warning, error, critical, alert, emergency. It also accepts the slog
// offset syntax such as "INFO+2".
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "notice":
		return LevelNotice, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	case "critical":
		return LevelCritical, nil
	case "alert":
		return LevelAlert, nil
	case "emergency":
		return LevelEmergency, nil
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("log: invalid level %q", s)
	}
	return l, nil
}
