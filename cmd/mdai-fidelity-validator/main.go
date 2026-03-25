package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mydecisive/mdai-fidelity-validator/internal/validator"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		_, _ = os.Stderr.WriteString("failed to initialize logger\n")
		return
	}
	defer func() {
		_ = logger.Sync()
	}()

	if err := run(logger); err != nil {
		logger.Error("mdai-fidelity-validator exited with error", zap.Error(err))
		return
	}
}

func run(logger *zap.Logger) error {
	adminAddr := envOrDefault("MDAI_ADMIN_ADDR", ":8080")
	ingestAddr := envOrDefault("MDAI_INGEST_ADDR", ":8126")
	datadogAPIAddr := envOrDefault("MDAI_DATADOG_API_ADDR", ":8081")
	retention := durationEnvOrDefault("MDAI_RETENTION", 30*time.Minute)
	receiverUpstream := os.Getenv("MDAI_RECEIVER_UPSTREAM")
	exporterUpstream := os.Getenv("MDAI_EXPORTER_UPSTREAM")

	svc, err := validator.NewService(retention, receiverUpstream, exporterUpstream)
	if err != nil {
		return err
	}

	adminServer := &http.Server{
		Addr:              adminAddr,
		Handler:           svc.AdminRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ingestServer := &http.Server{
		Addr:              ingestAddr,
		Handler:           svc.IngestRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	datadogAPIServer := &http.Server{
		Addr:              datadogAPIAddr,
		Handler:           svc.DatadogAPIRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 3)
	go serve("admin", adminServer, logger, errCh)
	go serve("ingest", ingestServer, logger, errCh)
	go serve("datadog-api", datadogAPIServer, logger, errCh)

	logger.Info("mdai-fidelity-validator started",
		zap.String("admin", adminAddr),
		zap.String("ingest", ingestAddr),
		zap.String("datadog_api", datadogAPIAddr),
	)

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var serveErr error
	select {
	case <-stopCtx.Done():
	case serveErr = <-errCh:
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, server := range []*http.Server{adminServer, ingestServer, datadogAPIServer} {
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("shutdown error", zap.String("addr", server.Addr), zap.Error(err))
		}
	}

	return serveErr
}

func serve(name string, server *http.Server, logger *zap.Logger, errCh chan<- error) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		wrappedErr := fmt.Errorf("%s server failed: %w", name, err)
		logger.Error("server failed", zap.String("server", name), zap.Error(err))
		select {
		case errCh <- wrappedErr:
		default:
		}
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
		log.Printf("invalid duration for %s=%q, using default %s", key, value, fallback) //nolint:gosec
	}

	return fallback
}
