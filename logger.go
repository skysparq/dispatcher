package dispatcher

import "fmt"

type StandardLogger struct{}

func (l *StandardLogger) Errorf(format string, args ...interface{}) {
	println(fmt.Sprintf(`ERROR `+format, args...))
}

type NoopLogger struct{}

func (l *NoopLogger) Errorf(_ string, _ ...interface{}) {}
