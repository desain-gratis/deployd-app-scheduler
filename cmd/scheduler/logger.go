package main

import (
	"fmt"
	"log/slog"
	"os"
)

func logInject(logger *slog.Logger) {
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

}
