package main

import (
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

func logger(id string) *slog.Logger {
	outLog := &lumberjack.Logger{
		Filename: "/var/log/deployd_scheduler/queue/" + id + "/log.log", // TODO: id validation / log directory validation
	}

	logger := slog.New(slog.NewJSONHandler(outLog, nil))

	if value := os.Getenv("DEPLOYD_HOST"); value != "" {
		logger = logger.With("host", value)
	}

	if value := os.Getenv("DEPLOYD_SERVICE_NAMESPACE"); value != "" {
		logger = logger.With("svc_ns", value)
	}

	if value := os.Getenv("DEPLOYD_SERVICE"); value != "" {
		logger = logger.With("svc", value)
	}

	if value := os.Getenv("DEPLOYD_SERVICE_DEPLOY_ID"); value != "" {
		logger = logger.With("svc_did", value)
	}

	if value := os.Getenv("DEPLOYD_SERVICE_BUILD_COMMIT_ID"); value != "" {
		logger = logger.With("svc_commit", fmt.Sprintf("%.6s", value))
	}

	return logger
}
