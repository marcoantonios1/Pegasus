package wasource

import (
	"fmt"
	"regexp"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// longDigits matches runs of 8+ digits: phone numbers and LIDs as they
// appear in whatsmeow's own log lines (e.g. "from 4676…:1@lid"). Shorter
// runs (ports, counts, status codes) are left alone.
var longDigits = regexp.MustCompile(`[0-9]{8,}`)

// redactDigits keeps only the last 4 digits of each long digit run, the
// same short form Pegasus's own log lines use.
func redactDigits(s string) string {
	return longDigits.ReplaceAllStringFunc(s, func(m string) string { return "…" + m[len(m)-4:] })
}

// redactingLogger wraps whatsmeow's logger so its own lines never carry a
// full phone number or LID. It formats first, then redacts, then hands the
// finished line to the inner logger as a constant format.
type redactingLogger struct{ inner waLog.Logger }

func newWhatsmeowLogger(level string) waLog.Logger {
	return redactingLogger{inner: waLog.Stdout("whatsmeow", level, false)}
}

func (r redactingLogger) line(msg string, args []any) string {
	return redactDigits(fmt.Sprintf(msg, args...))
}

func (r redactingLogger) Warnf(msg string, args ...any)  { r.inner.Warnf("%s", r.line(msg, args)) }
func (r redactingLogger) Errorf(msg string, args ...any) { r.inner.Errorf("%s", r.line(msg, args)) }
func (r redactingLogger) Infof(msg string, args ...any)  { r.inner.Infof("%s", r.line(msg, args)) }
func (r redactingLogger) Debugf(msg string, args ...any) { r.inner.Debugf("%s", r.line(msg, args)) }
func (r redactingLogger) Sub(module string) waLog.Logger {
	return redactingLogger{inner: r.inner.Sub(module)}
}
