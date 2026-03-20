package plugin

import (
	"fmt"
	"log"

	"github.com/hollis-labs/fragments-engine/plugin"
)

// Logger implements plugin.Logger using Go's standard log package.
type Logger struct {
	prefix string
}

// NewLogger creates a new plugin logger with an optional prefix.
func NewLogger(prefix string) plugin.Logger {
	return &Logger{prefix: prefix}
}

func (l *Logger) Debug(msg string, keysAndValues ...interface{}) {
	l.logWithLevel("DEBUG", msg, keysAndValues...)
}

func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	l.logWithLevel("INFO", msg, keysAndValues...)
}

func (l *Logger) Warn(msg string, keysAndValues ...interface{}) {
	l.logWithLevel("WARN", msg, keysAndValues...)
}

func (l *Logger) Error(msg string, keysAndValues ...interface{}) {
	l.logWithLevel("ERROR", msg, keysAndValues...)
}

func (l *Logger) With(keysAndValues ...interface{}) plugin.Logger {
	newPrefix := l.prefix
	if len(keysAndValues) > 0 {
		newPrefix += " "
		for i := 0; i < len(keysAndValues); i += 2 {
			if i+1 < len(keysAndValues) {
				newPrefix += fmt.Sprint(keysAndValues[i]) + "=" + fmt.Sprint(keysAndValues[i+1]) + " "
			}
		}
	}
	return &Logger{prefix: newPrefix}
}

func (l *Logger) logWithLevel(level, msg string, keysAndValues ...interface{}) {
	fullMsg := level + " " + l.prefix + " " + msg
	if len(keysAndValues) > 0 {
		fullMsg += " |"
		for i := 0; i < len(keysAndValues); i += 2 {
			if i+1 < len(keysAndValues) {
				fullMsg += " " + fmt.Sprint(keysAndValues[i]) + "=" + fmt.Sprint(keysAndValues[i+1])
			}
		}
	}
	log.Println(fullMsg)
}
