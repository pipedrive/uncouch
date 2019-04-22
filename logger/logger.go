package logger

import (
	"sync"

	"go.uber.org/zap"
)

// Package internal variable to implement singleton
var (
	innerLogger *zap.Logger
	innerSugar  *zap.SugaredLogger

	onceLogger sync.Once
)

// GetLogger returns singleton logger object.
func GetLogger() (*zap.Logger, *zap.SugaredLogger) {
	var err error

	onceLogger.Do(func() {
		innerLogger, err = zap.NewDevelopment()
		if err != nil {
			panic("Unbale to create logger. Quit application.")
		}
		innerSugar = innerLogger.Sugar()
	})
	return innerLogger, innerSugar
}
