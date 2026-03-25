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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"slices"
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

const numShards = 32

type shard struct {
	mu         sync.Mutex
	pending    map[string]*observedPayload
	lastResult map[string]ComparisonResult
}

type Service struct {
	shards     []*shard
	retention  time.Duration
	httpClient *http.Client
	policy     Policy

	// Registry lock for global lookups/debug
	regMu        sync.Mutex
	lastBySource map[string]*observedPayload
	lastByKey    map[string]*observedPayload

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
	signal      Signal
	correlation string
	receivedAt  time.Time
	body        []byte
	format      string
	decodeError string
	request     RequestSnapshot
	flattened   map[string]string
}

type ComparisonResult struct {
	Signal            Signal                   `json:"signal"`
	CorrelationID     string                   `json:"correlation_id"`
	ReceiverFields    map[string]string        `json:"receiver_fields,omitempty"`
	ExporterFields    map[string]string        `json:"exporter_fields,omitempty"`
	ReceiverRawFields map[string]string        `json:"receiver_raw_fields,omitempty"`
	ExporterRawFields map[string]string        `json:"exporter_raw_fields,omitempty"`
	Passed            bool                     `json:"passed"`
	FullPayloadPassed bool                     `json:"full_payload_passed"`
	AttributeTotal    int                      `json:"attribute_total"`
	Matched           []string                 `json:"matched"`
	Mismatched        []AttributeDelta         `json:"mismatched,omitempty"`
	MissingIn         []MissingField           `json:"missing_in,omitempty"`
	RequiredChecks    []RequiredAttributeCheck `json:"required_checks,omitempty"`
	ReceiverWire      RequestSnapshot          `json:"receiver_wire"`
	ExporterWire      RequestSnapshot          `json:"exporter_wire"`
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
	Listener        string            `json:"listener,omitempty"`
	Method          string            `json:"method,omitempty"`
	Path            string            `json:"path,omitempty"`
	Query           string            `json:"query,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
	Format          string            `json:"format,omitempty"`
	DecodeError     string            `json:"decode_error,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
}

type DebugPayload struct {
	Source        string            `json:"source"`
	Signal        Signal            `json:"signal"`
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
	policy, policySource, err := loadPolicy()
	if err != nil {
		return nil, fmt.Errorf("load policy: %w", err)
	}
	log.Printf("loaded fidelity policy source=%s summary=%s", policySource, summarizePolicy(policy))

	svc := &Service{
		shards:           make([]*shard, numShards),
		retention:        retention,
		lastBySource:     make(map[string]*observedPayload),
		lastByKey:        make(map[string]*observedPayload),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		policy:           policy,
		receiverUpstream: receiverURL,
		exporterUpstream: exporterURL,
		receivedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_payloads_received_total",
			Help: "Number of payloads received by source and signal.",
		}, []string{"source", "signal"}),
		attributeEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_attribute_checks_total",
			Help: "Number of attribute comparisons by signal, attribute, and result.",
		}, []string{"signal", "attribute", "result"}),
		signalEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_signal_checks_total",
			Help: "Number of whole-signal comparisons by signal and result.",
		}, []string{"signal", "result"}),
		requiredEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_required_attribute_checks_total",
			Help: "Number of required attribute comparisons by signal, attribute, and result.",
		}, []string{"signal", "attribute", "result"}),
		requiredSig: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_required_signal_checks_total",
			Help: "Number of policy-based whole-signal comparisons by signal and result.",
		}, []string{"signal", "result"}),
		pendingGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "mdai_fidelity_pending_payloads",
			Help: "Number of payloads waiting for their correlated counterpart.",
		}),
	}

	for i := range numShards {
		svc.shards[i] = &shard{
			pending:    make(map[string]*observedPayload),
			lastResult: make(map[string]ComparisonResult),
		}
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

func (s *Service) IngestRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleProxyIngest)
	return mux
}

func (s *Service) DatadogAPIRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/validate", s.handleDatadogValidate)
	mux.HandleFunc("/", s.handleDatadogAPI)
	return mux
}

func (*Service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Service) handleResults(w http.ResponseWriter, r *http.Request) {
	correlationID := strings.TrimPrefix(r.URL.Path, "/results/")
	if correlationID == "" || correlationID == "results" {
		http.Error(w, "missing correlation id", http.StatusBadRequest)
		return
	}

	sh := s.getShard(correlationID)
	sh.mu.Lock()
	result, ok := sh.lastResult[correlationID]
	sh.mu.Unlock()
	if !ok {
		http.Error(w, "result not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Service) handleDebugPending(w http.ResponseWriter, _ *http.Request) {
	var pending []DebugPayload
	for _, sh := range s.shards {
		sh.mu.Lock()
		for _, payload := range sh.pending {
			pending = append(pending, debugPayloadFromObserved(payload))
		}
		sh.mu.Unlock()
	}

	slices.SortFunc(pending, func(a, b DebugPayload) int {
		switch {
		case a.ReceivedAt.Before(b.ReceivedAt):
			return -1
		case a.ReceivedAt.After(b.ReceivedAt):
			return 1
		case a.CorrelationID < b.CorrelationID:
			return -1
		case a.CorrelationID > b.CorrelationID:
			return 1
		default:
			return 0
		}
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

	s.regMu.Lock()
	payload, ok := s.lastBySource[source]
	s.regMu.Unlock()
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

	s.regMu.Lock()
	payload, ok := s.lastByKey[parts[0]+":"+parts[1]]
	s.regMu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, debugPayloadFromObserved(payload))
}

func (s *Service) handleDebugResults(w http.ResponseWriter, _ *http.Request) {
	var results []ComparisonResult
	for _, sh := range s.shards {
		sh.mu.Lock()
		for _, result := range sh.lastResult {
			results = append(results, result)
		}
		sh.mu.Unlock()
	}

	slices.SortFunc(results, func(a, b ComparisonResult) int {
		switch {
		case a.ComparedAt.Before(b.ComparedAt):
			return -1
		case a.ComparedAt.After(b.ComparedAt):
			return 1
		default:
			return 0
		}
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(results),
		"results": results,
	})
}

func (s *Service) handleDatadogValidate(w http.ResponseWriter, r *http.Request) {
	s.captureDatadogAPIRequest(":8443", r)
	writeJSON(w, http.StatusOK, map[string]any{
		"valid": true,
	})
}

func (s *Service) handleDatadogAPI(w http.ResponseWriter, r *http.Request) {
	path := readRequestPath(r)
	signal := inferSignalFromDatadogPath(path)
	if signal != "unknown" {
		observed, _, _, err := s.captureRequest("exporter", signal, ":8081", path, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if s.exporterUpstream == nil {
			writeDatadogAck(w, signal)
			return
		}

		resp, err := s.forwardRaw(r.Context(), s.exporterUpstream, path, r.URL.RawQuery, r.Method, r.Header, observed.body)
		if err != nil {
			http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close() //nolint:errcheck

		copyResponse(w, resp)
		return
	}

	//nolint:gosec // debug log for unsupported paths; request details are intentionally logged.
	log.Printf("rejected datadog-api request listener=%s method=%s path=%s reason=%q headers=%v",
		":8081", r.Method, path, "unsupported path on datadog-api listener", selectedHeaders(r.Header))
	http.Error(w, "unsupported path", http.StatusNotFound)
}

func (s *Service) handleSource(source string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		signal := strings.TrimPrefix(r.URL.Path, "/intake/"+source+"/")
		if signal == "" || strings.Contains(signal, "/") {
			http.Error(w, "signal must be one of traces, metrics, or logs", http.StatusBadRequest)
			return
		}

		observed, result, matched, err := s.captureRequest(source, Signal(signal), "admin", readRequestPath(r), r)
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
		if observed.decodeError != "" {
			response["decode_error"] = observed.decodeError
		}
		if result != nil {
			response["comparison"] = result
		}

		writeJSON(w, http.StatusAccepted, response)
	}
}

func (s *Service) handleProxyIngest(w http.ResponseWriter, r *http.Request) {
	path := readRequestPath(r)
	signal := inferSignalFromDatadogPath(path)
	observed, _, _, err := s.captureRequest("receiver", signal, ":8126", path, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	upstream := s.receiverUpstream
	if upstream == nil {
		writeDatadogAck(w, signal)
		return
	}

	resp, err := s.forwardRaw(r.Context(), upstream, path, r.URL.RawQuery, r.Method, r.Header, observed.body)
	if err != nil {
		http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	copyResponse(w, resp)
}

func (s *Service) getShard(correlationID string) *shard {
	hash := sha256.Sum256([]byte(correlationID))
	index := int(hash[0]) % numShards
	return s.shards[index]
}

func (s *Service) observe(payload *observedPayload) (*ComparisonResult, bool) {
	sh := s.getShard(payload.correlation)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	s.gcShardLocked(sh, time.Now().UTC())
	s.rememberObserved(payload)

	if existing, ok := sh.pending[payload.correlation]; ok {
		if existing.source == payload.source {
			sh.pending[payload.correlation] = payload
			s.updatePendingGauge()
			return nil, false
		}

		delete(sh.pending, payload.correlation)
		s.updatePendingGauge()

		result := comparePair(existing, payload, s.policy)
		sh.lastResult[payload.correlation] = result
		s.recordMetrics(result)
		return &result, true
	}

	sh.pending[payload.correlation] = payload
	s.updatePendingGauge()
	return nil, false
}

func (s *Service) rememberObserved(payload *observedPayload) {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	s.lastBySource[payload.source] = payload
	s.lastByKey[payload.source+":"+string(payload.signal)] = payload
}

func (s *Service) gcShardLocked(sh *shard, now time.Time) {
	for key, payload := range sh.pending {
		if now.Sub(payload.receivedAt) > s.retention {
			delete(sh.pending, key)
		}
	}
	for key, result := range sh.lastResult {
		if now.Sub(result.ComparedAt) > s.retention {
			delete(sh.lastResult, key)
		}
	}
}

func (s *Service) updatePendingGauge() {
	var total int
	for _, sh := range s.shards {
		total += len(sh.pending)
	}
	s.pendingGauge.Set(float64(total))
}

func (s *Service) recordMetrics(result ComparisonResult) {
	for _, attribute := range result.Matched {
		s.attributeEval.WithLabelValues(string(result.Signal), attribute, "pass").Inc()
	}
	for _, delta := range result.Mismatched {
		s.attributeEval.WithLabelValues(string(result.Signal), delta.Attribute, "fail").Inc()
	}
	for _, missing := range result.MissingIn {
		s.attributeEval.WithLabelValues(string(result.Signal), missing.Attribute, "fail").Inc()
	}

	if result.FullPayloadPassed {
		s.signalEval.WithLabelValues(string(result.Signal), "pass").Inc()
	} else {
		s.signalEval.WithLabelValues(string(result.Signal), "fail").Inc()
	}

	if result.Passed {
		s.requiredSig.WithLabelValues(string(result.Signal), "pass").Inc()
	} else {
		s.requiredSig.WithLabelValues(string(result.Signal), "fail").Inc()
	}
	for _, check := range result.RequiredChecks {
		resultLabel := "fail"
		if check.Passed {
			resultLabel = "pass"
		}
		s.requiredEval.WithLabelValues(string(result.Signal), check.Attribute, resultLabel).Inc()
	}
}

func comparePair(a, b *observedPayload, policy Policy) ComparisonResult {
	receiver := a
	exporter := b
	if receiver.source != "receiver" {
		receiver, exporter = exporter, receiver
	}

	receiverCompareFields := normalizeFieldsForComparison(receiver.signal, receiver.flattened)
	exporterCompareFields := normalizeFieldsForComparison(receiver.signal, exporter.flattened)

	result := ComparisonResult{
		Signal:            receiver.signal,
		CorrelationID:     receiver.correlation,
		ReceiverFields:    receiverCompareFields,
		ExporterFields:    exporterCompareFields,
		ReceiverRawFields: receiver.flattened,
		ExporterRawFields: exporter.flattened,
		ReceiverWire:      receiver.request,
		ExporterWire:      exporter.request,
		ComparedAt:        time.Now().UTC(),
	}

	allKeysMap := make(map[string]struct{}, len(receiverCompareFields)+len(exporterCompareFields))
	for key := range receiverCompareFields {
		allKeysMap[key] = struct{}{}
	}
	for key := range exporterCompareFields {
		allKeysMap[key] = struct{}{}
	}

	allKeys := make([]string, 0, len(allKeysMap))
	for key := range allKeysMap {
		allKeys = append(allKeys, key)
	}
	slices.Sort(allKeys)

	for _, key := range allKeys {
		receiverValue, receiverOK := receiverCompareFields[key]
		exporterValue, exporterOK := exporterCompareFields[key]

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

func normalizeFieldsForComparison(signal Signal, fields map[string]string) map[string]string {
	normalized := make(map[string]string, len(fields))
	for key, value := range fields {
		normalized[normalizeFieldKeyForComparison(signal, key)] = value
	}
	return normalized
}

func normalizeFieldKeyForComparison(signal Signal, key string) string {
	// Logs from datadogexporter often arrive as a single-item array payload:
	// [0].message, [0].service, [0].ddtags, etc.
	// Normalize that wrapper so receiver-side "message" compares against exporter-side "[0].message".
	if signal == "logs" && strings.HasPrefix(key, "[0].") {
		return strings.TrimPrefix(key, "[0].")
	}
	return key
}

func (s *Service) captureRequest(source string, signal Signal, listener, path string, r *http.Request) (*observedPayload, *ComparisonResult, bool, error) {
	body, err := io.ReadAll(http.MaxBytesReader(noopResponseWriter{}, r.Body, 10<<20))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	decodedBody, format, err := decodeBody(body, path, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type"))
	fields := map[string]string{}
	decodeError := ""
	if err != nil {
		format = "raw"
		decodeError = fmt.Sprintf("failed to decode payload: %v", err)
	} else {
		fields = flattenValueMap(decodedBody)
	}

	correlationID := deriveCorrelationFromFields(signal, fields)
	if correlationID == "" {
		if headerCorrelationID := r.Header.Get("X-Correlation-ID"); headerCorrelationID != "" {
			correlationID = string(signal) + ":" + headerCorrelationID
		} else if len(fields) > 0 {
			correlationID = deriveFingerprintCorrelationID(signal, fields)
		} else {
			correlationID = deriveRawBodyCorrelationID(signal, body)
		}
	}

	observed := &observedPayload{
		source:      source,
		signal:      signal,
		correlation: correlationID,
		receivedAt:  time.Now().UTC(),
		body:        body,
		format:      format,
		decodeError: decodeError,
		request: RequestSnapshot{
			Listener:        listener,
			Method:          r.Method,
			Path:            path,
			Query:           r.URL.RawQuery,
			ContentType:     r.Header.Get("Content-Type"),
			ContentEncoding: r.Header.Get("Content-Encoding"),
			Format:          format,
			DecodeError:     decodeError,
			Headers:         selectedHeaders(r.Header),
		},
		flattened: fields,
	}

	s.receivedTotal.WithLabelValues(source, string(signal)).Inc()
	logObservedPayload(observed)
	if decodeError != "" {
		s.rememberObserved(observed)
		return observed, nil, false, nil
	}
	result, matched := s.observe(observed)
	if matched && result != nil && source == "exporter" {
		logComparisonSummary(*result)
	}
	return observed, result, matched, nil
}

func (s *Service) captureDatadogAPIRequest(listener string, r *http.Request) {
	body := mustReadBodyBytes(r)
	path := readRequestPath(r)
	signal := inferDatadogAPISignal(path)
	format := "raw"
	fields := map[string]string{}

	if len(body) > 0 {
		if decodedBody, decodedFormat, err := decodeBody(body, path, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type")); err == nil {
			fields = flattenValueMap(decodedBody)
			format = decodedFormat
		}
	}

	observed := &observedPayload{
		source:      "datadog-api",
		signal:      signal,
		correlation: "datadog-api:" + string(signal),
		receivedAt:  time.Now().UTC(),
		body:        body,
		format:      format,
		request: RequestSnapshot{
			Listener:        listener,
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

	logObservedPayload(observed)
	s.rememberObserved(observed)
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
		slices.Sort(keys)
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

func deriveCorrelationFromFields(signal Signal, fields map[string]string) string {
	for _, key := range correlationCandidates(fields) {
		if candidate := correlationValueForField(key, fields[key]); candidate != "" {
			return string(signal) + ":" + candidate
		}
	}
	return ""
}

func deriveFingerprintCorrelationID(signal Signal, fields map[string]string) string {
	// Stable Identity Fields by signal type
	identityFields := map[Signal][]string{
		SignalTraces:  {"trace_id", "traceID", "[0][0].trace_id"},
		SignalMetrics: {"series[0].metric", "series[0].points[0][0]"},
		SignalLogs:    {"message", "timestamp", "attributes.http.url"},
	}

	builder := strings.Builder{}
	builder.WriteString(string(signal))

	foundIdentity := false
	if keys, ok := identityFields[signal]; ok {
		for _, key := range keys {
			if val, ok := fields[key]; ok && val != "" {
				builder.WriteString("|" + key + "=" + val)
				foundIdentity = true
			}
		}
	}

	if !foundIdentity {
		stableTags := []string{"service", "env", "version", "meta.service", "meta.env"}
		for _, tag := range stableTags {
			for fieldKey, val := range fields {
				if strings.Contains(fieldKey, "tags") && strings.Contains(val, tag+":") {
					builder.WriteString("|" + fieldKey + "=" + val)
				}
			}
		}
	}

	hash := sha256.Sum256([]byte(builder.String()))
	return string(signal) + ":fp:" + hex.EncodeToString(hash[:12])
}

func deriveRawBodyCorrelationID(signal Signal, body []byte) string {
	hash := sha256.Sum256(body)
	return string(signal) + ":" + hex.EncodeToString(hash[:8])
}

func inferSignalFromDatadogPath(path string) Signal {
	switch {
	case strings.HasSuffix(path, "/traces"):
		return SignalTraces
	case strings.HasSuffix(path, "/series"), strings.HasSuffix(path, "/check_run"), strings.HasSuffix(path, "/sketches"), strings.HasSuffix(path, "/distribution_points"):
		return SignalMetrics
	case strings.HasSuffix(path, "/logs"):
		return SignalLogs
	default:
		return SignalUnknown
	}
}

func inferDatadogAPISignal(path string) Signal {
	switch path {
	case "/api/v1/validate":
		return SignalValidate
	default:
		return SignalAPI
	}
}

func decodeBody(body []byte, path, contentEncoding, contentType string) (any, string, error) {
	var decoded []byte
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
	switch {
	case strings.HasSuffix(path, "/api/v2/series"):
		payload := &agentpayload.MetricPayload{}
		if err := payload.Unmarshal(body); err != nil {
			return nil, "", err
		}
		return protobufToMap(payload)
	case strings.HasSuffix(path, "/api/v1/sketches"), strings.HasSuffix(path, "/api/beta/sketches"):
		payload := &agentpayload.SketchPayload{}
		if err := payload.Unmarshal(body); err != nil {
			return nil, "", err
		}
		return protobufToMap(payload)
	case strings.HasSuffix(path, "/api/v0.2/traces"):
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
	slices.Sort(keys)

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
		default:
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
		default:
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
		for part := range strings.SplitSeq(value, ",") {
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
	defer reader.Close() //nolint:errcheck

	return io.ReadAll(reader)
}

func inflate(body []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close() //nolint:errcheck

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
		return nil, nil //nolint:nilnil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("must include scheme and host")
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
		log.Printf("failed to read request body: %v", err)
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
	return s.httpClient.Do(req) //nolint:gosec
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

func writeDatadogAck(w http.ResponseWriter, signal Signal) {
	w.Header().Set("Content-Type", "application/json")
	switch signal {
	case SignalTraces:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"rate_by_service":{}}`))
	case SignalMetrics, SignalLogs:
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	case SignalAPI, SignalValidate, SignalUnknown:
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

func logObservedPayload(payload *observedPayload) {
	debug := debugPayloadFromObserved(payload)
	body, err := json.Marshal(debug)
	if err != nil {
		log.Printf("captured payload source=%s signal=%s correlation_id=%s marshal_error=%v", payload.source, payload.signal, payload.correlation, err)
		return
	}
	log.Printf("captured payload %s", string(body))
}

func logComparisonSummary(result ComparisonResult) {
	requiredPassed := 0
	for _, check := range result.RequiredChecks {
		if check.Passed {
			requiredPassed++
		}
	}

	log.Printf(
		"comparison result signal=%s correlation_id=%s policy_pass=%t full_payload_pass=%t matched=%d mismatched=%d missing=%d required_passed=%d required_total=%d",
		result.Signal,
		result.CorrelationID,
		result.Passed,
		result.FullPayloadPassed,
		len(result.Matched),
		len(result.Mismatched),
		len(result.MissingIn),
		requiredPassed,
		len(result.RequiredChecks),
	)
}

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header       { return make(http.Header) }
func (noopResponseWriter) Write([]byte) (int, error) { return 0, nil }
func (noopResponseWriter) WriteHeader(int)           {}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

func summarizePolicy(policy Policy) string {
	signals := make([]string, 0, len(policy.Signals))
	for signal, signalPolicy := range policy.Signals {
		signals = append(signals, fmt.Sprintf("%s:%d", signal, len(signalPolicy.RequiredAttributes)))
	}
	slices.Sort(signals)
	return strings.Join(signals, ",")
}
