package wasource

import (
	"fmt"
	"strings"
	"testing"

	waLog "go.mau.fi/whatsmeow/util/log"
)

type captureLogger struct {
	module string
	lines  *[]string
}

func (c captureLogger) add(level, msg string, args []any) {
	*c.lines = append(*c.lines, level+" "+c.module+" "+fmt.Sprintf(msg, args...))
}
func (c captureLogger) Warnf(m string, a ...any)  { c.add("WARN", m, a) }
func (c captureLogger) Errorf(m string, a ...any) { c.add("ERROR", m, a) }
func (c captureLogger) Infof(m string, a ...any)  { c.add("INFO", m, a) }
func (c captureLogger) Debugf(m string, a ...any) { c.add("DEBUG", m, a) }
func (c captureLogger) Sub(m string) waLog.Logger {
	return captureLogger{module: c.module + "/" + m, lines: c.lines}
}

func TestRedactingLogger(t *testing.T) {
	var lines []string
	l := redactingLogger{inner: captureLogger{module: "whatsmeow", lines: &lines}}.Sub("Client")

	// The real line observed in production, with made-up numbers.
	l.Warnf("Unavailable message %s from %s in %s (type: %q)", "3EB0B2D6150982F965AD45", "46761234567:1@lid", "46761234567@lid", "")
	l.Errorf("failed for 15551234567@s.whatsapp.net after %d tries on port %d (status 401)", 3, 8095)
	l.Infof("100%% literal percent, no args")

	all := strings.Join(lines, "\n")
	for _, leak := range []string{"46761234567", "15551234567"} {
		if strings.Contains(all, leak) {
			t.Errorf("full number leaked:\n%s", all)
		}
	}
	for _, want := range []string{"…4567:1@lid", "…4567@s.whatsapp.net", "3 tries", "port 8095", "status 401", "WARN whatsmeow/Client", "100% literal percent"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in:\n%s", want, all)
		}
	}
}
