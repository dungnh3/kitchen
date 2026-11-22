package log

import (
	"bytes"
	"runtime/debug"
	"strings"
)

// loggingFrames are the function-name prefixes of frames that belong to the
// logging machinery and are cut from the top of a stack trace.
var loggingFrames = []string{
	"runtime/debug.",
	"log/slog.",
	"github.com/dungnh3/kitchen/log.",
	"go.uber.org/zap.",
	"go.uber.org/zap/zapcore.",
}

// stackTrace returns the current goroutine's stack in the format of
// runtime/debug.Stack (the format Go panics use and Google Cloud Error
// Reporting parses), without the frames of the logging machinery on top.
func stackTrace() string {
	raw := debug.Stack()
	header, rest, ok := bytes.Cut(raw, []byte("\n"))
	if !ok {
		return string(raw)
	}
	// After the "goroutine N [running]:" header, each frame is two lines: the
	// function call and "\tfile:line +0xoff".
	lines := strings.Split(strings.TrimRight(string(rest), "\n"), "\n")
	i := 0
	for i+1 < len(lines) && isLoggingFrame(lines[i]) {
		i += 2
	}
	var b strings.Builder
	b.Grow(len(raw))
	b.Write(header)
	for _, l := range lines[i:] {
		b.WriteByte('\n')
		b.WriteString(l)
	}
	return b.String()
}

func isLoggingFrame(call string) bool {
	for _, p := range loggingFrames {
		if strings.HasPrefix(call, p) {
			return true
		}
	}
	return false
}
