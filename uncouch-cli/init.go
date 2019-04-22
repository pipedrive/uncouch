package main

import (
	"github.com/pipedrive/uncouch/logger"
	"go.uber.org/zap"
)

var (
	log  *zap.Logger
	slog *zap.SugaredLogger
)

func init() {
	log, slog = logger.GetLogger()
}
