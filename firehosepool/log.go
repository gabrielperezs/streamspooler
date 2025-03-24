package firehosepool

import (
	"log/slog"
	"os"
)

type Logger interface {
	Error(msg string, args ...any)
	Info(msg string, args ...any)
	Debug(msg string, args ...any)
}

type defaultLogger struct {
	log   Logger
	level slog.Leveler
}

var logger defaultLogger

func SetLogger(l Logger, level slog.Leveler) {
	logger.log = l
	if l == nil {
		ho := &slog.HandlerOptions{
			AddSource: false,
			Level:     level,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				return a
				// return slog.Attr{}
				// if a.Key != slog.TimeKey {
				// 	return a
				// }
				// t := a.Value.Time()
				// a.Value = slog.StringValue(t.Format(time.DateTime))
				// return a
			},
		}
		handler := slog.NewTextHandler(os.Stderr, ho)
		logger.log = slog.New(handler)
	}
	logger.level = level
}

func (d *defaultLogger) Error(msg string, args ...any) {
	d.log.Error(msg, args...)
}
func (d *defaultLogger) Info(msg string, args ...any) {
	d.log.Info(msg, args...)
}
func (d *defaultLogger) Debug(msg string, args ...any) {
	// We do not send Debug logs even with custom logger
	if logger.level == slog.LevelDebug {
		d.log.Debug(msg, args...)
	}
}
