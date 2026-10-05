package logger

import (
	"context"
	"log/slog"
	"os"
)

type handler struct {
	h     slog.Handler
	close func()
}

type Logger struct {
	log   *slog.Logger
	scope string
	close func()
}

func NewLogger(scope, logFile string) (*Logger, error) {
	handler, err := loggerHandler(logFile)
	if err != nil {
		return nil, err
	}

	return &Logger{
		log:   slog.New(handler.h),
		scope: scope,
		close: handler.close,
	}, nil
}

func (l *Logger) WithScope(scope string) *Logger {
	return &Logger{
		log:   l.log,
		scope: scope,
		close: l.close,
	}
}

func (l *Logger) Info(ctx context.Context, message string, tags ...slog.Attr) {
	l.logWitAttrs(ctx, slog.LevelInfo, message, tags...)
}

func (l *Logger) Error(ctx context.Context, message string, err error, tags ...slog.Attr) {
	if err != nil {
		tags = append(tags, slog.Any("error", err))
	}
	l.logWitAttrs(ctx, slog.LevelError, message, tags...)
}

func (l *Logger) ShutDown() {
	l.close()
}

func (l *Logger) logWitAttrs(ctx context.Context, level slog.Level, message string, tags ...slog.Attr) {
	if l.log == nil || l.log.Handler() == nil {
		return
	}
	tags = append(tags, slog.String("Scope", l.scope))
	l.log.LogAttrs(ctx, level, message, tags...)
}

func loggerHandler(logFile string) (*handler, error) {
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0664)
	if err != nil {
		return nil, err
	}
	return &handler{
		h: slog.NewJSONHandler(file, nil),
		close: func() {
			file.Sync()
			file.Close()
		},
	}, nil
}
