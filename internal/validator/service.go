package validator

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	agentpayload "github.com/DataDog/agent-payload/v5/gogen"
	tracepb "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmihailenco/msgpack/v5"
)

type Service struct {
	mu           sync.Mutex
	retention    time.Duration
	pending      map[string]*observedPayload
	lastResult   map[string]ComparisonResult
	lastBySource map[string]*observedPayload
	lastByKey    map[string]*observedPayload
	httpClient   *http.Client
	policy       Policy

	receiverUpstream *url.URL
	exporterUpstream *url.URL

	receivedTotal *prometheus.CounterVec
	attributeEval *prometheus.CounterVec
	signalEval    *prometheus.CounterVec
	requiredEval  *prometheus.CounterVec
	requiredSig   *prometheus.CounterVec
	pendingGauge  prometheus.Gauge
}

type observedPayload struct {
	source      string
	signal      string
	correlation string
	receivedAt  time.Time
	body        []byte
	format      string
	request     RequestSnapshot
	flattened   map[string]string
}

type ComparisonResult struct {
	Signal            string                   `json:"signal"`
	CorrelationID     string                   `json:"correlation_id"`
	ReceiverFields    map[string]string        `json:"receiver_fields,omitempty"`
	ExporterFields    map[string]string        `json:"exporter_fields,omitempty"`
	Passed            bool                     `json:"passed"`
	FullPayloadPassed bool                     `json:"full_payload_passed"`
	AttributeTotal    int                      `json:"attribute_total"`
	Matched           []string                 `json:"matched"`
	Mismatched        []AttributeDelta         `json:"mismatched,omitempty"`
	MissingIn         []MissingField           `json:"missing_in,omitempty"`
	RequiredChecks    []RequiredAttributeCheck `json:"required_checks,omitempty"`
	ReceiverWire      RequestSnapshot          `json:"receiver_wire,omitempty"`
	ExporterWire      RequestSnapshot          `json:"exporter_wire,omitempty"`
	ComparedAt        time.Time                `json:"compared_at"`
}

type AttributeDelta struct {
	Attribute string `json:"attribute"`
	Receiver  string `json:"receiver"`
	Exporter  string `json:"exporter"`
}

type MissingField struct {
	Attribute string `json:"attribute"`
	Side      string `json:"side"`
	Value     string `json:"value"`
}

type RequestSnapshot struct {
	Method          string            `json:"method,omitempty"`
	Path            string            `json:"path,omitempty"`
	Query           string            `json:"query,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
	Format          string            `json:"format,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
}

type DebugPayload struct {
	Source        string            `json:"source"`
	Signal        string            `json:"signal"`
	CorrelationID string            `json:"correlation_id"`
	ReceivedAt    time.Time         `json:"received_at"`
	Format        string            `json:"format"`
	Request       RequestSnapshot   `json:"request"`
	Attributes    map[string]string `json:"attributes"`
	RawBody       string            `json:"raw_body,omitempty"`
}

func NewService(retention time.Duration, receiverUpstream, exporterUpstream string) (*Service, error) {
	receiverURL, err := parseOptionalURL(receiverUpstream)
	if err != nil {
		return nil, fmt.Errorf("invalid receiver upstream: %w", err)
	}
	exporterURL, err := parseOptionalURL(exporterUpstream)
	if err != nil {
		return nil, fmt.Errorf("invalid exporter upstream: %w", err)
	}
	policy, err := loadPolicy()
	if err != nil {
		return nil, fmt.Errorf("load policy: %w", err)
	}

	svc := &Service{
		retention:        retention,
		pending:          make(map[string]*observedPayload),
		lastResult:       make(map[string]ComparisonResult),
		lastBySource:     make(map[string]*observedPayload),
		lastByKey:        make(map[string]*observedPayload),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		policy:           policy,
		receiverUpstream: receiverURL,
		exporterUpstream: exporterURL,
		receivedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_dd_fidelity_payloads_received_total",
			Help: "Number of payloads received by source and signal.",
		}, []string{"source", "signal"}),
		attributeEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_dd_fidelity_attribute_checks_total",
			Help: "Number of attribute comparisons by signal, attribute, and result.",
		}, []string{"signal", "attribute", "result"}),
		signalEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_dd_fidelity_signal_checks_total",
			Help: "Number of whole-signal comparisons by signal and result.",
		}, []string{"signal", "result"}),
		requiredEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_dd_fidelity_required_attribute_checks_total",
			Help: "Number of required attribute comparisons by signal, attribute, and result.",
		}, []string{"signal", "attribute", "result"}),
		requiredSig: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_dd_fidelity_required_signal_checks_total",
			Help: "Number of policy-based whole-signal comparisons by signal and result.",
		}, []string{"signal", "result"}),
		pendingGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "mdai_dd_fidelity_pending_payloads",
			Help: "Number of payloads waiting for their correlated counterpart.",
		}),
	}

	return svc, nil
}

func (s *Service) AdminRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/debug/pending", s.handleDebugPending)
	mux.HandleFunc("/debug/last/", s.handleDebugLast)
	mux.HandleFunc("/debug/last-signal/", s.handleDebugLastSignal)
	mux.HandleFunc("/debug/results", s.handleDebugResults)
	mux.HandleFunc("/intake/receiver/", s.handleSource("receiver"))
	mux.HandleFunc("/intake/exporter/", s.handleSource("exporter"))
	mux.HandleFunc("/results/", s.handleResults)

	return mux
}

func (s *Service) ProxyRoutes(source string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleProxySource(source))
	return mux
}

func (s *Service) DatadogAPIRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/validate", s.handleDatadogValidate)
	mux.HandleFunc("/", s.handleDatadogAPI)
	return mux
}

func (s *Service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Service) handleResults(w http.ResponseWriter, r *http.Request) {
	correlationID := strings.TrimPrefix(r.URL.Path, "/results/")
	if correlationID == "" || correlationID == "results" {
		http.Error(w, "missing correlation id", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	result, ok := s.lastResult[correlationID]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "result not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Service) handleDebugPending(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pending := make([]DebugPayload, 0, len(s.pending))
	for _, payload := range s.pending {
		pending = append(pending, debugPayloadFromObserved(payload))
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].ReceivedAt.Equal(pending[j].ReceivedAt) {
			return pending[i].CorrelationID < pending[j].CorrelationID
		}
		return pending[i].ReceivedAt.Before(pending[j].ReceivedAt)
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(pending),
		"pending": pending,
	})
}

func (s *Service) handleDebugLast(w http.ResponseWriter, r *http.Request) {
	source := strings.TrimPrefix(r.URL.Path, "/debug/last/")
	if source == "" || strings.Contains(source, "/") {
		http.Error(w, "missing source", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	payload, ok := s.lastBySource[source]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, debugPayloadFromObserved(payload))
}

func (s *Service) handleDebugLastSignal(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/debug/last-signal/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "expected /debug/last-signal/{source}/{signal}", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	payload, ok := s.lastByKey[parts[0]+":"+parts[1]]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, debugPayloadFromObserved(payload))
}

func (s *Service) handleDebugResults(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.lastResult))
	for key := range s.lastResult {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	results := make([]ComparisonResult, 0, len(keys))
	for _, key := range keys {
		results = append(results, s.lastResult[key])
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(results),
		"results": results,
	})
}

func (s *Service) handleDatadogValidate(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"valid": true,
	})
}

func (s *Service) handleDatadogAPI(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"emulated": true,
		"path":     r.URL.Path,
	})
}

func (s *Service) handleSource(source string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		signal := strings.TrimPrefix(r.URL.Path, "/intake/"+source+"/")
		if signal == "" || strings.Contains(signal, "/") {
			http.Error(w, "signal must be one of traces, metrics, or logs", http.StatusBadRequest)
			return
		}

		observed, result, matched, err := s.captureRequest(source, signal, readRequestPath(r), r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		response := map[string]any{
			"source":         source,
			"signal":         signal,
			"correlation_id": observed.correlation,
			"format":         observed.format,
			"attributes":     observed.flattened,
			"matched":        matched,
		}
		if result != nil {
			response["comparison"] = result
		}

		writeJSON(w, http.StatusAccepted, response)
	}
}

func (s *Service) handleProxySource(source string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := readRequestPath(r)
		signal := inferSignalFromDatadogPath(path)
		_, _, _, err := s.captureRequest(source, signal, path, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		upstream := s.receiverUpstream
		if source == "exporter" {
			upstream = s.exporterUpstream
		}
		if upstream == nil {
			writeDatadogAck(w, signal)
			return
		}

		resp, err := s.forwardRaw(r.Context(), upstream, path, r.URL.RawQuery, r.Method, r.Header, mustReadBodyBytes(r))
		if err != nil {
			http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		copyResponse(w, resp)
	}
}

func (s *Service) observe(payload *observedPayload) (*ComparisonResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gcLocked(time.Now().UTC())
	s.lastBySource[payload.source] = payload
	s.lastByKey[payload.source+":"+payload.signal] = payload

	if existing, ok := s.pending[payload.correlation]; ok {
		if existing.source == payload.source {
			s.pending[payload.correlation] = payload
			s.pendingGauge.Set(float64(len(s.pending)))
			return nil, false
		}

		delete(s.pending, payload.correlation)
		s.pendingGauge.Set(float64(len(s.pending)))

		result := comparePair(existing, payload, s.policy)
		s.lastResult[payload.correlation] = result
		s.recordMetrics(result)
		return &result, true
	}

	s.pending[payload.correlation] = payload
	s.pendingGauge.Set(float64(len(s.pending)))
	return nil, false
}

func (s *Service) gcLocked(now time.Time) {
	for key, payload := range s.pending {
		if now.Sub(payload.receivedAt) > s.retention {
			delete(s.pending, key)
		}
	}
	s.pendingGauge.Set(float64(len(s.pending)))
}

func (s *Service) recordMetrics(result ComparisonResult) {
	for _, attribute := range result.Matched {
		s.attributeEval.WithLabelValues(result.Signal, attribute, "pass").Inc()
	}
	for _, delta := range result.Mismatched {
		s.attributeEval.WithLabelValues(result.Signal, delta.Attribute, "fail").Inc()
	}
	for _, missing := range result.MissingIn {
		s.attributeEval.WithLabelValues(result.Signal, missing.Attribute, "fail").Inc()
	}

	if result.FullPayloadPassed {
		s.signalEval.WithLabelValues(result.Signal, "pass").Inc()
	} else {
		s.signalEval.WithLabelValues(result.Signal, "fail").Inc()
	}

	if result.Passed {
		s.requiredSig.WithLabelValues(result.Signal, "pass").Inc()
	} else {
		s.requiredSig.WithLabelValues(result.Signal, "fail").Inc()
	}
	for _, check := range result.RequiredChecks {
		resultLabel := "fail"
		if check.Passed {
			resultLabel = "pass"
		}
		s.requiredEval.WithLabelValues(result.Signal, check.Attribute, resultLabel).Inc()
	}
}

func comparePair(a, b *observedPayload, policy Policy) ComparisonResult {
	receiver := a
	exporter := b
	if receiver.source != "receiver" {
		receiver, exporter = exporter, receiver
	}

	result := ComparisonResult{
		Signal:         receiver.signal,
		CorrelationID:  receiver.correlation,
		ReceiverFields: receiver.flattened,
		ExporterFields: exporter.flattened,
		ReceiverWire:   receiver.request,
		ExporterWire:   exporter.request,
		ComparedAt:     time.Now().UTC(),
	}

	allKeysMap := make(map[string]struct{}, len(receiver.flattened)+len(exporter.flattened))
	for key := range receiver.flattened {
		allKeysMap[key] = struct{}{}
	}
	for key := range exporter.flattened {
		allKeysMap[key] = struct{}{}
	}

	allKeys := make([]string, 0, len(allKeysMap))
	for key := range allKeysMap {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)

	for _, key := range allKeys {
		receiverValue, receiverOK := receiver.flattened[key]
		exporterValue, exporterOK := exporter.flattened[key]

		switch {
		case receiverOK && exporterOK && receiverValue == exporterValue:
			result.Matched = append(result.Matched, key)
		case receiverOK && exporterOK:
			result.Mismatched = append(result.Mismatched, AttributeDelta{
				Attribute: key,
				Receiver:  receiverValue,
				Exporter:  exporterValue,
			})
		case receiverOK:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "exporter",
				Value:     receiverValue,
			})
		default:
			result.MissingIn = append(result.MissingIn, MissingField{
				Attribute: key,
				Side:      "receiver",
				Value:     exporterValue,
			})
		}
	}

	result.AttributeTotal = len(allKeys)
	result.FullPayloadPassed = len(result.Mismatched) == 0 && len(result.MissingIn) == 0
	result.RequiredChecks, result.Passed = evaluatePolicy(result.Signal, receiver.flattened, exporter.flattened, policy)
	return result
}

func (s *Service) captureRequest(source, signal, path string, r *http.Request) (*observedPayload, *ComparisonResult, bool, error) {
	body, err := io.ReadAll(http.MaxBytesReader(noopResponseWriter{}, r.Body, 10<<20))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	decodedBody, format, err := decodeBody(body, path, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to decode payload: %w", err)
	}

	fields := flattenValueMap(decodedBody)
	correlationID := deriveCorrelationFromFields(signal, fields)
	if correlationID == "" {
		if headerCorrelationID := r.Header.Get("X-Correlation-ID"); headerCorrelationID != "" {
			correlationID = signal + ":" + headerCorrelationID
		} else {
			correlationID = deriveFingerprintCorrelationID(signal, fields)
		}
	}

	observed := &observedPayload{
		source:      source,
		signal:      signal,
		correlation: correlationID,
		receivedAt:  time.Now().UTC(),
		body:        body,
		format:      format,
		request: RequestSnapshot{
			Method:          r.Method,
			Path:            path,
			Query:           r.URL.RawQuery,
			ContentType:     r.Header.Get("Content-Type"),
			ContentEncoding: r.Header.Get("Content-Encoding"),
			Format:          format,
			Headers:         selectedHeaders(r.Header),
		},
		flattened: fields,
	}

	s.receivedTotal.WithLabelValues(source, signal).Inc()
	result, matched := s.observe(observed)
	return observed, result, matched, nil
}

func flattenValueMap(payload any) map[string]string {
	result := make(map[string]string)
	flattenValue(result, "", payload)
	return result
}

func flattenValue(result map[string]string, prefix string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			flattenValue(result, next, typed[key])
		}
	case []any:
		for index, item := range typed {
			next := prefix + "[" + strconv.Itoa(index) + "]"
			flattenValue(result, next, item)
		}
	case string:
		result[prefix] = typed
	case float64:
		result[prefix] = strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		result[prefix] = strconv.FormatBool(typed)
	case nil:
		result[prefix] = "null"
	default:
		result[prefix] = fmt.Sprintf("%v", typed)
	}
}

func deriveCorrelationFromFields(signal string, fields map[string]string) string {
	for _, key := range correlationCandidates(fields) {
		if candidate := correlationValueForField(key, fields[key]); candidate != "" {
			return signal + ":" + candidate
		}
	}
	return ""
}

func deriveFingerprintCorrelationID(signal string, fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	builder := strings.Builder{}
	builder.WriteString(signal)
	for _, key := range keys {
		builder.WriteString("|")
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(fields[key])
	}

	hash := sha256.Sum256([]byte(builder.String()))
	return signal + ":" + hex.EncodeToString(hash[:8])
}

func inferSignalFromDatadogPath(path string) string {
	switch {
	case path == "/v0.3/traces", path == "/v0.4/traces", path == "/v0.5/traces", path == "/v0.7/traces", path == "/api/v0.2/traces":
		return "traces"
	case path == "/api/v1/series", path == "/api/v2/series", path == "/api/v1/check_run", path == "/api/v1/sketches", path == "/api/beta/sketches", path == "/api/v1/distribution_points":
		return "metrics"
	case path == "/api/v2/logs":
		return "logs"
	default:
		return "unknown"
	}
}

func decodeBody(body []byte, path, contentEncoding, contentType string) (any, string, error) {
	decoded := body
	format := "json"

	if decodedBody, err := decodeCompression(body, contentEncoding); err == nil {
		decoded = decodedBody
	} else {
		return nil, "", err
	}

	if strings.Contains(strings.ToLower(contentType), "protobuf") {
		payload, protoFormat, err := decodeDatadogProtobuf(path, decoded)
		if err != nil {
			return nil, "", err
		}
		return payload, protoFormat, nil
	}

	if looksLikeJSON(decoded) || strings.Contains(strings.ToLower(contentType), "json") {
		var payload any
		if err := json.Unmarshal(decoded, &payload); err == nil {
			return payload, format, nil
		}
	}

	var payload any
	if err := msgpack.Unmarshal(decoded, &payload); err == nil {
		return normalizeMsgpackValue(payload), "msgpack", nil
	}

	return nil, "", fmt.Errorf("unsupported payload encoding or content type %q", contentType)
}

func decodeCompression(body []byte, contentEncoding string) ([]byte, error) {
	switch strings.ToLower(contentEncoding) {
	case "", "identity":
		if isGzip(body) {
			return gunzip(body)
		}
		return body, nil
	case "gzip":
		return gunzip(body)
	case "deflate":
		return inflate(body)
	default:
		return nil, fmt.Errorf("unsupported content encoding %q", contentEncoding)
	}
}

func decodeDatadogProtobuf(path string, body []byte) (any, string, error) {
	switch path {
	case "/api/v2/series":
		payload := &agentpayload.MetricPayload{}
		if err := payload.Unmarshal(body); err != nil {
			return nil, "", err
		}
		return protobufToMap(payload)
	case "/api/v0.2/traces":
		payload := &tracepb.AgentPayload{}
		if err := payload.UnmarshalVT(body); err != nil {
			return nil, "", err
		}
		return protobufToMap(payload)
	default:
		return nil, "", fmt.Errorf("unsupported protobuf path %q", path)
	}
}

func protobufToMap(message any) (map[string]any, string, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return nil, "", err
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", err
	}
	return payload, "protobuf", nil
}

func normalizeMsgpackValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			out[key] = normalizeMsgpackValue(inner)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			out[fmt.Sprintf("%v", key)] = normalizeMsgpackValue(inner)
		}
		return out
	case []any:
		for i := range typed {
			typed[i] = normalizeMsgpackValue(typed[i])
		}
		return typed
	case []byte:
		return string(typed)
	default:
		return typed
	}
}

func correlationCandidates(fields map[string]string) []string {
	exact := []string{
		"meta.correlation_id",
		"meta.fidelity.correlation_id",
		"attributes.correlation_id",
		"attributes.fidelity.correlation_id",
		"correlation_id",
		"fidelity.correlation_id",
		"trace_id",
		"resource.trace_id",
		"span.trace_id",
		"0.trace_id",
		"resource.correlation_id",
		"resource.fidelity.correlation_id",
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var candidates []string
	seen := make(map[string]struct{})
	add := func(key string) {
		if _, ok := seen[key]; ok {
			return
		}
		if _, ok := fields[key]; ok {
			seen[key] = struct{}{}
			candidates = append(candidates, key)
		}
	}

	for _, key := range exact {
		add(key)
	}

	for _, key := range keys {
		lower := strings.ToLower(key)
		valueLower := strings.ToLower(fields[key])
		switch {
		case strings.HasSuffix(lower, ".correlation_id"),
			strings.Contains(lower, ".correlation_id."),
			strings.HasSuffix(lower, ".fidelity.correlation_id"),
			strings.Contains(lower, "correlationid"),
			strings.Contains(lower, "meta.correlation_id"),
			strings.Contains(lower, ".attributes.correlation_id"),
			strings.Contains(lower, ".attributes.fidelity.correlation_id"),
			strings.HasSuffix(lower, ".ddtags"),
			lower == "ddtags",
			strings.Contains(lower, ".tags[") && strings.Contains(valueLower, "correlation_id:"):
			if strings.Contains(lower, ".resource.") || strings.HasPrefix(lower, "resource.") {
				continue
			}
			add(key)
		}
	}

	for _, key := range keys {
		lower := strings.ToLower(key)
		switch {
		case strings.HasSuffix(lower, ".trace_id"),
			strings.Contains(lower, ".resource.correlation_id"),
			strings.Contains(lower, ".resource.fidelity.correlation_id"),
			strings.HasPrefix(lower, "resource.correlation_id"),
			strings.HasPrefix(lower, "resource.fidelity.correlation_id"):
			add(key)
		}
	}

	for _, key := range []string{"series[0].metric"} {
		add(key)
	}

	return candidates
}

func correlationValueForField(key, value string) string {
	lowerKey := strings.ToLower(key)
	if strings.Contains(lowerKey, ".tags[") {
		if parsed := parseCorrelationTag(value); parsed != "" {
			return parsed
		}
	}
	if strings.HasSuffix(lowerKey, ".ddtags") || lowerKey == "ddtags" || strings.HasSuffix(lowerKey, "ddtags") {
		for _, part := range strings.Split(value, ",") {
			if parsed := parseCorrelationTag(strings.TrimSpace(part)); parsed != "" {
				return parsed
			}
		}
	}
	return value
}

func parseCorrelationTag(tag string) string {
	switch {
	case strings.HasPrefix(tag, "correlation_id:"):
		return strings.TrimPrefix(tag, "correlation_id:")
	case strings.HasPrefix(tag, "fidelity.correlation_id:"):
		return strings.TrimPrefix(tag, "fidelity.correlation_id:")
	default:
		return ""
	}
}

func looksLikeJSON(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	return trimmed[0] == '{' || trimmed[0] == '['
}

func isGzip(body []byte) bool {
	return len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b
}

func gunzip(body []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func inflate(body []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func selectedHeaders(header http.Header) map[string]string {
	keys := []string{
		"Content-Type",
		"Content-Encoding",
		"User-Agent",
		"DD-API-KEY",
		"DD-Agent-Version",
		"Datadog-Meta-Tracer-Version",
		"Datadog-Meta-Lang",
		"Datadog-Meta-Lang-Version",
		"X-Correlation-ID",
	}

	out := make(map[string]string)
	for _, key := range keys {
		if value := header.Get(key); value != "" {
			out[key] = value
		}
	}
	return out
}

func parseOptionalURL(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("must include scheme and host")
	}
	return parsed, nil
}

func readRequestPath(r *http.Request) string {
	if r.URL.Path == "" {
		return "/"
	}
	return r.URL.Path
}

func mustReadBodyBytes(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body
}

func (s *Service) forwardRaw(ctx context.Context, upstream *url.URL, path, rawQuery, method string, header http.Header, body []byte) (*http.Response, error) {
	target := *upstream
	target.Path = joinURLPath(upstream.Path, path)
	target.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = cloneHeaders(header)
	req.Host = upstream.Host
	return s.httpClient.Do(req)
}

func joinURLPath(basePath, requestPath string) string {
	basePath = strings.TrimSuffix(basePath, "/")
	if requestPath == "" {
		requestPath = "/"
	}
	if strings.HasPrefix(requestPath, "/") {
		return basePath + requestPath
	}
	return basePath + "/" + requestPath
}

func cloneHeaders(header http.Header) http.Header {
	cloned := make(http.Header, len(header))
	for key, values := range header {
		for _, value := range values {
			cloned.Add(key, value)
		}
	}
	return cloned
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("failed to copy upstream response body: %v", err)
	}
}

func writeDatadogAck(w http.ResponseWriter, signal string) {
	w.Header().Set("Content-Type", "application/json")
	switch signal {
	case "traces":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"rate_by_service":{}}`))
	case "metrics", "logs":
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	default:
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"captured"}`))
	}
}

func debugPayloadFromObserved(payload *observedPayload) DebugPayload {
	rawBody := string(payload.body)
	if !utf8.Valid(payload.body) {
		rawBody = base64.StdEncoding.EncodeToString(payload.body)
	}

	return DebugPayload{
		Source:        payload.source,
		Signal:        payload.signal,
		CorrelationID: payload.correlation,
		ReceivedAt:    payload.receivedAt,
		Format:        payload.format,
		Request:       payload.request,
		Attributes:    payload.flattened,
		RawBody:       rawBody,
	}
}

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header        { return make(http.Header) }
func (noopResponseWriter) Write([]byte) (int, error)  { return 0, nil }
func (noopResponseWriter) WriteHeader(statusCode int) {}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}
