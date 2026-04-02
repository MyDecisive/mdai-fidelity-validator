package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mydecisive/mdai-fidelity-validator/internal/ddgen"
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

	if err := run(); err != nil {
		logger.Error("ddmltgen failed", zap.Error(err))
	}
}

func run() error {
	var (
		baseURL             = flag.String("endpoint", "http://localhost:8126", "Base URL for the Datadog ingest endpoint")
		mirrorExporterURL   = flag.String("mirror-exporter-endpoint", "", "Optional second endpoint to send a correlated request to")
		signal              = flag.String("signal", "random", "Signal to send: traces, metrics, logs, random")
		encoding            = flag.String("encoding", "random", "Payload encoding: json, msgpack, random")
		count               = flag.Int("count", 1, "Number of requests to send")
		interval            = flag.Duration("interval", 0, "Delay between requests")
		useGzip             = flag.Bool("gzip", false, "Compress payloads with gzip")
		omitCorrelationID   = flag.Bool("omit-correlation-id", false, "Do not include correlation_id/fidelity.correlation_id in payloads or X-Correlation-ID header")
		service             = flag.String("service", "", "Service name override")
		env                 = flag.String("env", "dev", "Environment tag")
		host                = flag.String("host", "localhost", "Hostname value")
		dropAttrProbability = flag.Float64("drop-attr-probability", 0, "When mirroring to the exporter endpoint, randomly drop non-correlation attributes with this probability")
		userAgent           = flag.String("user-agent", "mdai-fidelity-validator/ddmltgen", "User-Agent header")
		fidelitySide        = flag.String("fidelity-side", "", "Optional X-Fidelity-Side header for the primary request: receiver or exporter")
		mirrorFidelitySide  = flag.String("mirror-fidelity-side", "exporter", "Optional X-Fidelity-Side header for the mirrored request: receiver, exporter, or empty to omit")
	)
	flag.Parse() //nolint:revive // argument parsing is delegated to run() for testable main flow.

	target, err := url.Parse(*baseURL)
	if err != nil {
		return err
	}
	var mirrorTarget *url.URL
	if *mirrorExporterURL != "" {
		mirrorTarget, err = url.Parse(*mirrorExporterURL)
		if err != nil {
			return err
		}
	}

	parsedSignal, err := chooseSignal(*signal)
	if err != nil {
		return err
	}
	parsedEncoding, err := chooseEncoding(*encoding)
	if err != nil {
		return err
	}
	primaryFidelitySide, err := normalizeFidelitySide(*fidelitySide)
	if err != nil {
		return err
	}
	mirrorParsedFidelitySide, err := normalizeFidelitySide(*mirrorFidelitySide)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for i := range *count {
		ctx := context.Background()
		opts := ddgen.Options{
			Signal:          parsedSignal,
			Encoding:        parsedEncoding,
			Gzip:            *useGzip,
			Service:         *service,
			Environment:     *env,
			Host:            *host,
			OmitCorrelation: *omitCorrelationID,
		}
		reqSpec, err := ddgen.BuildRequest(opts)
		if err != nil {
			return err
		}

		resp, err := doRequest(ctx, client, target, reqSpec, *userAgent, primaryFidelitySide)
		if err != nil {
			return err
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_, _ = fmt.Fprintf(os.Stdout, "signal=%s encoding=%s correlation_id=%s status=%d path=%s body=%s\n",
			reqSpec.Signal, reqSpec.ContentType, reqSpec.CorrelationID, resp.StatusCode, reqSpec.Path, strings.TrimSpace(string(body)))

		if mirrorTarget != nil {
			mirrorReq, err := ddgen.BuildRequestWithCorrelation(opts, reqSpec.CorrelationID, *dropAttrProbability)
			if err != nil {
				return err
			}
			mirrorResp, err := doRequest(ctx, client, mirrorTarget, mirrorReq, *userAgent, mirrorParsedFidelitySide)
			if err != nil {
				return err
			}
			mirrorBody, _ := io.ReadAll(mirrorResp.Body)
			_ = mirrorResp.Body.Close()
			_, _ = fmt.Fprintf(os.Stdout, "mirror signal=%s encoding=%s correlation_id=%s status=%d path=%s body=%s\n",
				mirrorReq.Signal, mirrorReq.ContentType, mirrorReq.CorrelationID, mirrorResp.StatusCode, mirrorReq.Path, strings.TrimSpace(string(mirrorBody))) //nolint:errcheck
		}

		if i < *count-1 && *interval > 0 {
			time.Sleep(*interval)
		}
	}

	return nil
}

func doRequest(ctx context.Context, client *http.Client, target *url.URL, reqSpec ddgen.Request, userAgent, fidelitySide string) (*http.Response, error) {
	fullURL := target.ResolveReference(&url.URL{Path: reqSpec.Path})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL.String(), bytes.NewReader(reqSpec.Body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", string(reqSpec.ContentType))
	req.Header.Set("User-Agent", userAgent)
	if reqSpec.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", reqSpec.CorrelationID)
	}
	req.Header.Set("Dd-Api-Key", "demo-api-key")
	if fidelitySide != "" {
		req.Header.Set("X-Fidelity-Side", fidelitySide)
	}
	if reqSpec.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", string(reqSpec.ContentEncoding))
	}

	return client.Do(req)
}

func normalizeFidelitySide(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "receiver":
		return "receiver", nil
	case "exporter":
		return "exporter", nil
	default:
		return "", fmt.Errorf("unsupported fidelity side %q", raw)
	}
}

func chooseSignal(raw string) (ddgen.Signal, error) {
	switch strings.ToLower(raw) {
	case "traces":
		return ddgen.SignalTraces, nil
	case "metrics":
		return ddgen.SignalMetrics, nil
	case "logs":
		return ddgen.SignalLogs, nil
	case "random", "":
		options := []ddgen.Signal{ddgen.SignalTraces, ddgen.SignalMetrics, ddgen.SignalLogs}
		return options[rand.IntN(len(options))], nil //nolint:gosec
	default:
		return "", fmt.Errorf("unsupported signal %q", raw)
	}
}

func chooseEncoding(raw string) (ddgen.Encoding, error) {
	switch strings.ToLower(raw) {
	case "json", "":
		return ddgen.EncodingJSON, nil
	case "msgpack":
		return ddgen.EncodingMsgpack, nil
	case "random":
		options := []ddgen.Encoding{ddgen.EncodingJSON, ddgen.EncodingMsgpack}
		return options[rand.IntN(len(options))], nil //nolint:gosec
	default:
		return "", fmt.Errorf("unsupported encoding %q", raw)
	}
}
