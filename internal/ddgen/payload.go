package ddgen

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Encoding string

const (
	EncodingJSON    Encoding = "json"
	EncodingMsgpack Encoding = "msgpack"
)

type ContentType string

const (
	ContentTypeJSON    ContentType = "application/json"
	ContentTypeMsgpack ContentType = "application/msgpack"
)

type Signal string

const (
	SignalTraces  Signal = "traces"
	SignalMetrics Signal = "metrics"
	SignalLogs    Signal = "logs"
)

type ContentEncoding string

const (
	ContentEncodingGzip ContentEncoding = "gzip"
)

type Request struct {
	Path            string
	ContentType     ContentType
	ContentEncoding ContentEncoding
	Body            []byte
	CorrelationID   string
	Service         string
	Signal          Signal
}

type Options struct {
	Signal      Signal
	Encoding    Encoding
	Gzip        bool
	Service     string
	Environment string
	Host        string
}

func BuildRequest(opts Options) (Request, error) {
	payload, correlationID, path, contentType, err := buildPayload(opts)
	if err != nil {
		return Request{}, err
	}

	body, err := marshal(payload, opts.Encoding)
	if err != nil {
		return Request{}, err
	}

	var contentEncoding ContentEncoding
	if opts.Gzip {
		body, err = gzipBytes(body)
		if err != nil {
			return Request{}, err
		}
		contentEncoding = ContentEncodingGzip
	}

	return Request{
		Path:            path,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		Body:            body,
		CorrelationID:   correlationID,
		Service:         opts.Service,
		Signal:          opts.Signal,
	}, nil
}

func BuildRequestWithCorrelation(opts Options, correlationID string, dropAttrProbability float64) (Request, error) {
	payload, resolvedCorrelationID, path, contentType, err := buildPayload(opts, correlationID)
	if err != nil {
		return Request{}, err
	}

	if dropAttrProbability > 0 {
		mutatePayloadForDrop(opts.Signal, payload, dropAttrProbability)
	}

	body, err := marshal(payload, opts.Encoding)
	if err != nil {
		return Request{}, err
	}

	var contentEncoding ContentEncoding
	if opts.Gzip {
		body, err = gzipBytes(body)
		if err != nil {
			return Request{}, err
		}
		contentEncoding = ContentEncodingGzip
	}

	return Request{
		Path:            path,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		Body:            body,
		CorrelationID:   resolvedCorrelationID,
		Service:         opts.Service,
		Signal:          opts.Signal,
	}, nil
}

func buildPayload(opts Options, fixedCorrelationID ...string) (any, string, string, ContentType, error) {
	correlationID := "corr-" + randomHex(8)
	if len(fixedCorrelationID) > 0 && fixedCorrelationID[0] != "" {
		correlationID = fixedCorrelationID[0]
	}
	if opts.Service == "" {
		opts.Service = "ddgen-" + randomHex(4)
	}
	if opts.Environment == "" {
		opts.Environment = "dev"
	}
	if opts.Host == "" {
		opts.Host = "localhost"
	}

	var (
		path        string
		contentType ContentType
		payload     any
	)

	switch opts.Signal {
	case SignalTraces:
		path = "/v0.4/traces"
		contentType = ContentTypeJSON
		payload = buildTracePayload(opts.Service, opts.Environment, opts.Host, correlationID)
		if opts.Encoding == EncodingMsgpack {
			contentType = ContentTypeMsgpack
		}
	case SignalMetrics:
		if opts.Encoding == EncodingMsgpack {
			return nil, "", "", "", errors.New("metrics payloads currently support only json encoding")
		}
		path = "/api/v1/series"
		contentType = ContentTypeJSON
		payload = buildMetricsPayload(opts.Service, opts.Environment, opts.Host, correlationID)
	case SignalLogs:
		path = "/api/v2/logs"
		contentType = ContentTypeJSON
		payload = buildLogsPayload(opts.Service, opts.Environment, opts.Host, correlationID)
		if opts.Encoding == EncodingMsgpack {
			contentType = ContentTypeMsgpack
		}
	default:
		return nil, "", "", "", fmt.Errorf("unsupported signal %q", opts.Signal)
	}

	return payload, correlationID, path, contentType, nil
}

func marshal(payload any, encoding Encoding) ([]byte, error) {
	switch encoding {
	case "", EncodingJSON:
		return json.Marshal(payload)
	case EncodingMsgpack:
		return msgpack.Marshal(payload)
	default:
		return nil, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

func buildTracePayload(service, env, host, correlationID string) any {
	now := time.Now()
	start := now.Add(-250 * time.Millisecond).UnixNano()
	duration := int64(250 * time.Millisecond)
	traceID := randomUint63()
	parentSpanID := randomUint63()
	childSpanID := randomUint63()

	return [][]map[string]any{
		{
			{
				"trace_id": traceID,
				"span_id":  parentSpanID,
				"name":     "ddgen.request",
				"resource": "GET /checkout",
				"service":  service,
				"type":     "web",
				"start":    start,
				"duration": duration,
				"meta": map[string]any{
					"env":                     env,
					"host":                    host,
					"correlation_id":          correlationID,
					"fidelity.correlation_id": correlationID,
				},
				"metrics": map[string]any{
					"_sampling_priority_v1": 1,
				},
			},
			{
				"trace_id":  traceID,
				"parent_id": parentSpanID,
				"span_id":   childSpanID,
				"name":      "ddgen.db.query",
				"resource":  "SELECT * FROM orders",
				"service":   service,
				"type":      "sql",
				"start":     start + int64(50*time.Millisecond),
				"duration":  int64(100 * time.Millisecond),
				"meta": map[string]any{
					"env":                     env,
					"host":                    host,
					"correlation_id":          correlationID,
					"fidelity.correlation_id": correlationID,
				},
			},
		},
	}
}

func buildMetricsPayload(service, env, host, correlationID string) any {
	now := float64(time.Now().Unix())
	return map[string]any{
		"series": []map[string]any{
			{
				"metric": "ddgen.checkout.duration",
				"type":   "gauge",
				"host":   host,
				"points": [][]float64{{now, 123.45}},
				"tags": []string{
					"service:" + service,
					"env:" + env,
					"correlation_id:" + correlationID,
					"fidelity.correlation_id:" + correlationID,
				},
			},
		},
	}
}

func buildLogsPayload(service, env, host, correlationID string) any {
	return map[string]any{
		"message":        "ddgen synthetic log event",
		"service":        service,
		"hostname":       host,
		"ddsource":       "mdai-fidelity-validator",
		"ddtags":         "env:" + env + ",correlation_id:" + correlationID + ",fidelity.correlation_id:" + correlationID,
		"status":         "info",
		"timestamp":      time.Now().UTC().UnixMilli(),
		"correlation_id": correlationID,
		"attributes": map[string]any{
			"fidelity.correlation_id": correlationID,
			"env":                     env,
		},
	}
}

func gzipBytes(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return io.ReadAll(&buf)
}

func randomHex(n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func randomUint63() int64 {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	buf[0] &= 0x7f
	var out int64
	for _, b := range buf {
		out = (out << 8) | int64(b)
	}
	if out == 0 {
		return 1
	}
	return out
}

func mutatePayloadForDrop(signal Signal, payload any, probability float64) {
	if probability <= 0 {
		return
	}

	switch signal {
	case SignalTraces:
		traces, ok := payload.([][]map[string]any)
		if !ok {
			return
		}
		for i := range traces {
			for j := range traces[i] {
				dropMapKeys(traces[i][j], probability, map[string]struct{}{
					"trace_id": {},
					"span_id":  {},
					"name":     {},
					"resource": {},
					"service":  {},
					"start":    {},
					"duration": {},
					"meta":     {},
				})
				if meta, ok := traces[i][j]["meta"].(map[string]any); ok {
					dropMapKeys(meta, probability, protectedCorrelationKeys())
				}
			}
		}
	case SignalMetrics:
		metricPayload, ok := payload.(map[string]any)
		if !ok {
			return
		}
		series, ok := metricPayload["series"].([]map[string]any)
		if !ok {
			return
		}
		for i := range series {
			dropMapKeys(series[i], probability, map[string]struct{}{
				"metric": {},
				"type":   {},
				"points": {},
				"tags":   {},
			})
			if tags, ok := series[i]["tags"].([]string); ok {
				series[i]["tags"] = dropTags(tags, probability)
			}
		}
	case SignalLogs:
		logPayload, ok := payload.(map[string]any)
		if !ok {
			return
		}
		dropMapKeys(logPayload, probability, map[string]struct{}{
			"message":        {},
			"timestamp":      {},
			"correlation_id": {},
		})
		if attrs, ok := logPayload["attributes"].(map[string]any); ok {
			dropMapKeys(attrs, probability, protectedCorrelationKeys())
		}
	default:
	}
}

func dropMapKeys(values map[string]any, probability float64, protected map[string]struct{}) {
	for key := range values {
		if _, ok := protected[key]; ok {
			continue
		}
		if randFloat64() < probability {
			delete(values, key)
		}
	}
}

func dropTags(tags []string, probability float64) []string {
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		if strings.HasPrefix(tag, "correlation_id:") || strings.HasPrefix(tag, "fidelity.correlation_id:") {
			out = append(out, tag)
			continue
		}
		if randFloat64() < probability {
			continue
		}
		out = append(out, tag)
	}
	return out
}

func protectedCorrelationKeys() map[string]struct{} {
	return map[string]struct{}{
		"correlation_id":          {},
		"fidelity.correlation_id": {},
	}
}

func randFloat64() float64 {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	var n uint64
	for _, b := range buf {
		n = (n << 8) | uint64(b)
	}
	return float64(n>>11) / (1 << 53)
}
