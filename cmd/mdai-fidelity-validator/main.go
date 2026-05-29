package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mydecisive/mdai-fidelity-validator/internal/validator"
	"go.uber.org/zap"
)

type serverTimeouts struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

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
	metricsAddr := envOrDefault("MDAI_METRICS_ADDR", ":8888")
	ingestAddr := envOrDefault("MDAI_DATADOG_AGENT_INGEST_ADDR", ":8126")
	exporterAPIAddr := envOrDefault("MDAI_EXPORTER_API_ADDR", ":18081")
	retention := durationEnvOrDefault(logger, "MDAI_RETENTION", 30*time.Minute)

	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc, err := validator.NewService(stopCtx, logger, retention, ingestAddr, exporterAPIAddr)
	if err != nil {
		return err
	}

	adminServer := newHTTPServer(adminAddr, svc.AdminRoutes(), defaultServerTimeouts())
	metricsServer := newHTTPServer(metricsAddr, svc.MetricsRoutes(), defaultServerTimeouts())
	ingestServer := newHTTPServer(ingestAddr, svc.IngestRoutes(), defaultServerTimeouts())
	datadogAPIServer := newHTTPServer(exporterAPIAddr, svc.ExporterAPIRoutes(), defaultServerTimeouts())

	errCh := make(chan error, 4)
	go serve("admin", adminServer, logger, errCh)
	go serve("metrics", metricsServer, logger, errCh)
	go serve("datadog-agent-ingest", ingestServer, logger, errCh)
	go serve("exporter-api", datadogAPIServer, logger, errCh)

	logger.Info("mdai-fidelity-validator started",
		zap.String("admin", adminAddr),
		zap.String("metrics", metricsAddr),
		zap.String("datadog_agent_ingest", ingestAddr),
		zap.String("exporter_api", exporterAPIAddr),
	)

	var serveErr error
	select {
	case <-stopCtx.Done():
	case serveErr = <-errCh:
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, server := range []*http.Server{adminServer, metricsServer, ingestServer, datadogAPIServer} {
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

func durationEnvOrDefault(logger *zap.Logger, key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
		logger.Warn("invalid duration override, using default",
			zap.String("env_var", key),
			zap.String("value", value),
			zap.Duration("fallback", fallback),
		)
	}

	return fallback
}

func newHTTPServer(addr string, handler http.Handler, timeouts serverTimeouts) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: timeouts.ReadHeaderTimeout,
		ReadTimeout:       timeouts.ReadTimeout,
		WriteTimeout:      timeouts.WriteTimeout,
		IdleTimeout:       timeouts.IdleTimeout,
	}
}

func defaultServerTimeouts() serverTimeouts {
	return serverTimeouts{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
