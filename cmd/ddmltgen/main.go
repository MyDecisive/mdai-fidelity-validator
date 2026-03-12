package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"mdai-dd-fidelity-validator/internal/ddgen"
)

func main() {
	var (
		baseURL             = flag.String("endpoint", "http://localhost:8081", "Base URL for the Datadog ingress proxy")
		mirrorExporterURL   = flag.String("mirror-exporter-endpoint", "", "Optional exporter endpoint to send a second correlated request to, such as http://localhost:8082")
		signal              = flag.String("signal", "random", "Signal to send: traces, metrics, logs, random")
		encoding            = flag.String("encoding", "random", "Payload encoding: json, msgpack, random")
		count               = flag.Int("count", 1, "Number of requests to send")
		interval            = flag.Duration("interval", 0, "Delay between requests")
		useGzip             = flag.Bool("gzip", false, "Compress payloads with gzip")
		service             = flag.String("service", "", "Service name override")
		env                 = flag.String("env", "dev", "Environment tag")
		host                = flag.String("host", "localhost", "Hostname value")
		dropAttrProbability = flag.Float64("drop-attr-probability", 0, "When mirroring to the exporter endpoint, randomly drop non-correlation attributes with this probability")
		userAgent           = flag.String("user-agent", "mdai-dd-fidelity-validator/ddmltgen", "User-Agent header")
	)
	flag.Parse()

	target, err := url.Parse(*baseURL)
	if err != nil {
		log.Fatal(err)
	}
	var mirrorTarget *url.URL
	if *mirrorExporterURL != "" {
		mirrorTarget, err = url.Parse(*mirrorExporterURL)
		if err != nil {
			log.Fatal(err)
		}
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for i := 0; i < *count; i++ {
		opts := ddgen.Options{
			Signal:      chooseSignal(*signal),
			Encoding:    chooseEncoding(*encoding),
			Gzip:        *useGzip,
			Service:     *service,
			Environment: *env,
			Host:        *host,
		}
		reqSpec, err := ddgen.BuildRequest(opts)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := doRequest(client, target, reqSpec, *userAgent)
		if err != nil {
			log.Fatal(err)
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		fmt.Fprintf(os.Stdout, "signal=%s encoding=%s correlation_id=%s status=%d path=%s body=%s\n",
			reqSpec.Signal, reqSpec.ContentType, reqSpec.CorrelationID, resp.StatusCode, reqSpec.Path, strings.TrimSpace(string(body)))

		if mirrorTarget != nil {
			mirrorReq, err := ddgen.BuildRequestWithCorrelation(opts, reqSpec.CorrelationID, *dropAttrProbability)
			if err != nil {
				log.Fatal(err)
			}
			mirrorResp, err := doRequest(client, mirrorTarget, mirrorReq, *userAgent)
			if err != nil {
				log.Fatal(err)
			}
			mirrorBody, _ := io.ReadAll(mirrorResp.Body)
			_ = mirrorResp.Body.Close()
			fmt.Fprintf(os.Stdout, "mirror signal=%s encoding=%s correlation_id=%s status=%d path=%s body=%s\n",
				mirrorReq.Signal, mirrorReq.ContentType, mirrorReq.CorrelationID, mirrorResp.StatusCode, mirrorReq.Path, strings.TrimSpace(string(mirrorBody)))
		}

		if i < *count-1 && *interval > 0 {
			time.Sleep(*interval)
		}
	}
}

func doRequest(client *http.Client, target *url.URL, reqSpec ddgen.Request, userAgent string) (*http.Response, error) {
	fullURL := target.ResolveReference(&url.URL{Path: reqSpec.Path})
	req, err := http.NewRequest(http.MethodPost, fullURL.String(), bytes.NewReader(reqSpec.Body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", reqSpec.ContentType)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("X-Correlation-ID", reqSpec.CorrelationID)
	req.Header.Set("DD-API-KEY", "demo-api-key")
	if reqSpec.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", reqSpec.ContentEncoding)
	}

	return client.Do(req)
}

func chooseSignal(raw string) ddgen.Signal {
	switch strings.ToLower(raw) {
	case "traces":
		return ddgen.SignalTraces
	case "metrics":
		return ddgen.SignalMetrics
	case "logs":
		return ddgen.SignalLogs
	case "random", "":
		options := []ddgen.Signal{ddgen.SignalTraces, ddgen.SignalMetrics, ddgen.SignalLogs}
		return options[rand.IntN(len(options))]
	default:
		log.Fatalf("unsupported signal %q", raw)
		return ""
	}
}

func chooseEncoding(raw string) ddgen.Encoding {
	switch strings.ToLower(raw) {
	case "json", "":
		return ddgen.EncodingJSON
	case "msgpack":
		return ddgen.EncodingMsgpack
	case "random":
		options := []ddgen.Encoding{ddgen.EncodingJSON, ddgen.EncodingMsgpack}
		return options[rand.IntN(len(options))]
	default:
		log.Fatalf("unsupported encoding %q", raw)
		return ""
	}
}
