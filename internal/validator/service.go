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
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

const numShards = 32

const (
	defaultPairID       = "default"
	defaultTranslatorID = "datadog_raw"
	pairHeaderKey       = "X-Fidelity-Pair"
	configReloadEnvVar  = "MDAI_CONFIG_RELOAD_INTERVAL"
)

type shard struct {
	mu         sync.Mutex
	pending    map[string]*observedPayload
	lastResult map[string]ComparisonResult
}

type PayloadTranslator interface {
	Name() string
	Decode(signal Signal, path, contentEncoding, contentType string, body []byte) DecodedPayload
}

type DecodedPayload struct {
	Signal        Signal
	CorrelationID string
	Attributes    map[string]string
	Format        string
	DecodeError   string
}

type datadogRawTranslator struct {
	mapping *mappingStore
}

func (datadogRawTranslator) Name() string { return defaultTranslatorID }

func (t datadogRawTranslator) Decode(signal Signal, path, contentEncoding, contentType string, body []byte) DecodedPayload {
	decodedBody, format, err := decodeBody(body, path, contentEncoding, contentType)
	if err != nil {
		return DecodedPayload{
			Signal:      signal,
			Attributes:  map[string]string{},
			Format:      "raw",
			DecodeError: fmt.Sprintf("failed to decode payload: %v", err),
		}
	}
	canonicalSignal := signal
	if canonicalSignal == SignalUnknown {
		_, normalizedPath := parseExporterPath(path)
		canonicalSignal = inferSignalFromDatadogPath(normalizedPath)
	}
	fields := t.mapping.MapForPath(canonicalSignal, path, flattenValueMap(decodedBody))
	decodeError := ""
	if len(fields) == 0 {
		decodeError = "field mapping produced no canonical attributes"
	}
	return DecodedPayload{
		Signal:        canonicalSignal,
		CorrelationID: firstNonEmpty(fields["correlation_id"], fields["fidelity_correlation_id"]),
		Attributes:    fields,
		Format:        format,
		DecodeError:   decodeError,
	}
}

type PairConfig struct {
	ID                 string   `json:"id"`
	ReceiverTranslator string   `json:"receiver_translator"`
	ExporterTranslator string   `json:"exporter_translator"`
	ReceiverUpstream   string   `json:"receiver_upstream,omitempty"`
	ExporterUpstream   string   `json:"exporter_upstream,omitempty"`
	ReceiverPorts      []string `json:"receiver_ports,omitempty"`
	ExporterPorts      []string `json:"exporter_ports,omitempty"`
}

type configuredPair struct {
	PairConfig

	receiverUpstream *url.URL
	exporterUpstream *url.URL
}

type Service struct {
	logger     *zap.Logger
	shards     []*shard
	retention  time.Duration
	httpClient *http.Client
	policy     Policy
	policyMu   sync.RWMutex

	// Registry lock for global lookups/debug
	regMu        sync.Mutex
	lastBySource map[string]*observedPayload
	lastByKey    map[string]*observedPayload

	receiverUpstream   *url.URL
	exporterUpstream   *url.URL
	defaultPair        string
	translatorMu       sync.RWMutex
	translators        map[string]PayloadTranslator
	pairMu             sync.RWMutex
	pairs              map[string]configuredPair
	receiverPairByPort map[string]string
	exporterPairByPort map[string]string

	receivedTotal *prometheus.CounterVec
	attributeEval *prometheus.CounterVec
	signalEval    *prometheus.CounterVec
	requiredEval  *prometheus.CounterVec
	requiredSig   *prometheus.CounterVec
	pendingGauge  prometheus.Gauge
}

type mappingStore struct {
	mu      sync.RWMutex
	current FieldMapping
}

func newMappingStore(initial FieldMapping) *mappingStore {
	return &mappingStore{current: initial}
}

func (m *mappingStore) MapForPath(signal Signal, path string, fields map[string]string) map[string]string {
	m.mu.RLock()
	mapping := m.current
	m.mu.RUnlock()
	return mapping.MapForPath(signal, path, fields)
}

func (m *mappingStore) Set(next FieldMapping) {
	m.mu.Lock()
	m.current = next
	m.mu.Unlock()
}

type observedPayload struct {
	pair        string
	translator  string
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
	Pair          string            `json:"pair"`
	Translator    string            `json:"translator"`
	Source        string            `json:"source"`
	Signal        Signal            `json:"signal"`
	CorrelationID string            `json:"correlation_id"`
	ReceivedAt    time.Time         `json:"received_at"`
	Format        string            `json:"format"`
	Request       RequestSnapshot   `json:"request"`
	Attributes    map[string]string `json:"attributes"`
	RawBody       string            `json:"raw_body,omitempty"`
}

func NewService(logger *zap.Logger, retention time.Duration, receiverUpstream, exporterUpstream string) (*Service, error) {
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
	logger.Info("loaded fidelity policy", zap.String("source", policySource), zap.String("summary", summarizePolicy(policy)))
	fieldMap, mappingSource, err := loadFieldMapping()
	if err != nil {
		return nil, fmt.Errorf("load field mapping: %w", err)
	}
	logger.Info("loaded field mapping", zap.String("source", mappingSource))

	svc := &Service{
		logger:           logger,
		shards:           make([]*shard, numShards),
		retention:        retention,
		lastBySource:     make(map[string]*observedPayload),
		lastByKey:        make(map[string]*observedPayload),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
		policy:           policy,
		receiverUpstream: receiverURL,
		exporterUpstream: exporterURL,
		defaultPair:      defaultPairID,
		translators: map[string]PayloadTranslator{
			defaultTranslatorID: datadogRawTranslator{mapping: newMappingStore(fieldMap)},
		},
		pairs: map[string]configuredPair{
			defaultPairID: {
				PairConfig: PairConfig{
					ID:                 defaultPairID,
					ReceiverTranslator: defaultTranslatorID,
					ExporterTranslator: defaultTranslatorID,
				},
				receiverUpstream: receiverURL,
				exporterUpstream: exporterURL,
			},
		},
		receiverPairByPort: make(map[string]string),
		exporterPairByPort: make(map[string]string),
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
	svc.startConfigReloader()
	return svc, nil
}


func configSignature(v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) AdminRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/debug/pending", s.handleDebugPending)
	mux.HandleFunc("/debug/last/", s.handleDebugLast)
	mux.HandleFunc("/debug/last-signal/", s.handleDebugLastSignal)
	mux.HandleFunc("/debug/results", s.handleDebugResults)
	mux.HandleFunc("/debug/pairs", s.handleDebugPairs)
	mux.HandleFunc("/admin/pairs", s.handleAdminPairs)
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

func (s *Service) ExporterAPIRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/validate", s.handleDatadogValidate)
	mux.HandleFunc("/", s.handleExporterAPI)
	return mux
}

func (s *Service) currentPolicy() Policy {
	s.policyMu.RLock()
	defer s.policyMu.RUnlock()
	return s.policy
}

func (s *Service) setPolicy(next Policy) {
	s.policyMu.Lock()
	s.policy = next
	s.policyMu.Unlock()
}

func (s *Service) setMapping(next FieldMapping) {
	s.translatorMu.RLock()
	translator, ok := s.translators[defaultTranslatorID]
	s.translatorMu.RUnlock()
	if !ok {
		return
	}
	raw, ok := translator.(datadogRawTranslator)
	if !ok || raw.mapping == nil {
		return
	}
	raw.mapping.Set(next)
}

func (s *Service) startConfigReloader() {
	interval := 15 * time.Second
	if raw := strings.TrimSpace(os.Getenv(configReloadEnvVar)); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			s.logger.Warn("invalid config reload interval", zap.String("var", configReloadEnvVar), zap.String("value", raw), zap.Duration("default", interval))
		} else if parsed > 0 {
			interval = parsed
		}
	}

	var (
		lastPolicySig  string
		lastMappingSig string
	)
	reload := func() {
		policy, source, err := loadPolicy()
		if err != nil {
			s.logger.Warn("policy reload failed; keeping last-known-good", zap.Error(err))
		} else if sig, sigErr := configSignature(policy); sigErr == nil {
			if sig != lastPolicySig {
				s.setPolicy(policy)
				lastPolicySig = sig
				s.logger.Info("reloaded fidelity policy", zap.String("source", source), zap.String("summary", summarizePolicy(policy)))
			}
		}

		mapping, source, err := loadFieldMapping()
		if err != nil {
			s.logger.Warn("field mapping reload failed; keeping last-known-good", zap.Error(err))
		} else if sig, sigErr := configSignature(mapping); sigErr == nil {
			if sig != lastMappingSig {
				s.setMapping(mapping)
				lastMappingSig = sig
				s.logger.Info("reloaded field mapping", zap.String("source", source))
			}
		}
	}

	reload()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			reload()
		}
	}()
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

	writeJSON(s, w, http.StatusOK, result)
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

	writeJSON(s, w, http.StatusOK, map[string]any{
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

	writeJSON(s, w, http.StatusOK, debugPayloadFromObserved(payload))
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

	writeJSON(s, w, http.StatusOK, debugPayloadFromObserved(payload))
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

	writeJSON(s, w, http.StatusOK, map[string]any{
		"count":   len(results),
		"results": results,
	})
}

func (s *Service) handleDebugPairs(w http.ResponseWriter, _ *http.Request) {
	s.pairMu.RLock()
	defer s.pairMu.RUnlock()

	pairs := make([]map[string]any, 0, len(s.pairs))
	for _, pair := range s.pairs {
		pairs = append(pairs, map[string]any{
			"id":                  pair.ID,
			"receiver_translator": pair.ReceiverTranslator,
			"exporter_translator": pair.ExporterTranslator,
			"receiver_upstream":   pair.ReceiverUpstream,
			"exporter_upstream":   pair.ExporterUpstream,
			"receiver_ports":      pair.ReceiverPorts,
			"exporter_ports":      pair.ExporterPorts,
			"default":             pair.ID == s.defaultPair,
		})
	}
	slices.SortFunc(pairs, func(a, b map[string]any) int {
		return strings.Compare(fmt.Sprint(a["id"]), fmt.Sprint(b["id"]))
	})
	writeJSON(s, w, http.StatusOK, map[string]any{
		"default_pair": s.defaultPair,
		"pairs":        pairs,
	})
}

func (s *Service) handleAdminPairs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost, http.MethodPut:
	default:
		http.Error(w, "method not allowed, supported: [POST, PUT]", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PairConfig

		Default bool `json:"default"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	pair, err := s.newConfiguredPair(req.PairConfig)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.pairMu.Lock()
	s.pairs[pair.ID] = pair
	if req.Default {
		s.defaultPair = pair.ID
	}
	s.rebuildPortMappingsLocked()
	s.pairMu.Unlock()

	writeJSON(s, w, http.StatusAccepted, map[string]any{
		"saved":   true,
		"pair":    pair.ID,
		"default": req.Default,
	})
}

func (s *Service) handleDatadogValidate(w http.ResponseWriter, r *http.Request) {
	s.captureDatadogAPIRequest(":8443", r)
	writeJSON(s, w, http.StatusOK, map[string]any{
		"valid": true,
	})
}


func (s *Service) handleExporterAPI(w http.ResponseWriter, r *http.Request) {
	path := readRequestPath(r)
	_, normalizedPath := parseExporterPath(path)
	signal := inferSignalFromDatadogPath(normalizedPath)

	s.handleCommonIngest(w, r, "exporter", signal, ":18081", path, normalizedPath)
}

func (s *Service) handleProxyIngest(w http.ResponseWriter, r *http.Request) {
	path := readRequestPath(r)
	signal := inferSignalFromDatadogPath(path)

	s.handleCommonIngest(w, r, "receiver", signal, ":8126", path, path)
}

func (s *Service) handleCommonIngest(w http.ResponseWriter, r *http.Request, source string, signal Signal, listener, rawPath, forwardPath string) {
	pair := s.resolvePairForRequest(r, source, listener)
	translator := pair.ReceiverTranslator
	upstream := pair.receiverUpstream
	if source == "exporter" {
		translator = pair.ExporterTranslator
		upstream = pair.exporterUpstream
	}

	observed, _, _, err := s.captureRequest(pair.ID, translator, source, signal, listener, rawPath, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if upstream == nil {
		if source == "exporter" {
			upstream = s.exporterUpstream
		} else {
			upstream = s.receiverUpstream
		}
	}

	if upstream == nil {
		writeDatadogAck(w, signal)
		return
	}

	resp, err := s.forwardRaw(r.Context(), upstream, forwardPath, r.URL.RawQuery, r.Method, r.Header, observed.body)
	if err != nil {
		http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	copyResponse(s, w, resp)
}

func (s *Service) handleSource(source string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pair := s.resolvePairForRequest(r, source, "admin")
		signal := strings.TrimPrefix(r.URL.Path, "/intake/"+source+"/")
		if signal == "" || strings.Contains(signal, "/") {
			http.Error(w, "signal must be one of traces, metrics, or logs", http.StatusBadRequest)
			return
		}

		translator := pair.ReceiverTranslator
		if source == "exporter" {
			translator = pair.ExporterTranslator
		}
		observed, result, matched, err := s.captureRequest(pair.ID, translator, source, Signal(signal), "admin", readRequestPath(r), r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		response := map[string]any{
			"pair":           pair.ID,
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

		writeJSON(s, w, http.StatusAccepted, response)
	}
}

func (s *Service) getShard(correlationID string) *shard {
	index := int(xxhash.Sum64String(correlationID) % numShards)
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

		result := comparePair(existing, payload, s.currentPolicy())
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
		sh.mu.Lock()
		total += len(sh.pending)
		sh.mu.Unlock()
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
		normalizedKey := normalizeFieldKeyForComparison(signal, key)
		normalized[normalizedKey] = normalizeFieldValueForComparison(signal, normalizedKey, value)
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

func normalizeFieldValueForComparison(signal Signal, key, value string) string {
	if signal != SignalLogs {
		return value
	}

	lowerKey := strings.ToLower(key)
	switch {
	case lowerKey == "ddtags" || strings.HasSuffix(lowerKey, ".ddtags"):
		return stripCorrelationFromDDTags(value)
	case lowerKey == "message" || strings.HasSuffix(lowerKey, ".message"):
		return stripCorrelationFromMessageJSON(value)
	default:
		return value
	}
}

func stripCorrelationFromDDTags(tags string) string {
	if tags == "" {
		return tags
	}

	parts := strings.Split(tags, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		kv := strings.SplitN(trimmed, ":", 2)
		if len(kv) != 2 {
			if trimmed != "" {
				filtered = append(filtered, trimmed)
			}
			continue
		}
		key := strings.TrimSpace(kv[0])
		if key == "correlation_id" || key == "fidelity.correlation_id" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	return strings.Join(filtered, ",")
}

func stripCorrelationFromMessageJSON(message string) string {
	trimmed := strings.TrimSpace(message)
	if !strings.HasPrefix(trimmed, "{") {
		return message
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return message
	}
	delete(payload, "correlation_id")
	delete(payload, "fidelity.correlation_id")

	normalized, err := json.Marshal(payload)
	if err != nil {
		return message
	}
	return string(normalized)
}

func (s *Service) captureRequest(pairID, translatorID, source string, signal Signal, listener, path string, r *http.Request) (*observedPayload, *ComparisonResult, bool, error) {
	body, err := io.ReadAll(http.MaxBytesReader(noopResponseWriter{}, r.Body, 10<<20))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	translator, err := s.lookupTranslator(translatorID)
	if err != nil {
		return nil, nil, false, err
	}
	decoded := translator.Decode(signal, path, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type"), body)
	fields := decoded.Attributes
	if fields == nil {
		fields = map[string]string{}
	}
	effectiveSignal := decoded.Signal
	if effectiveSignal == SignalUnknown {
		effectiveSignal = signal
	}
	format := decoded.Format
	if format == "" {
		format = "raw"
	}
	decodeError := decoded.DecodeError
	if err := validateCanonicalAttributes(fields); err != nil {
		decodeError = firstNonEmpty(decodeError, fmt.Sprintf("invalid canonical attributes: %v", err))
	}

	correlationDecision := resolveCorrelationIDFromDecoded(effectiveSignal, decoded.CorrelationID, fields, r.Header, body)
	correlationID := correlationDecision.CorrelationID

	observed := &observedPayload{
		pair:        pairID,
		translator:  translator.Name(),
		source:      source,
		signal:      effectiveSignal,
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

	s.receivedTotal.WithLabelValues(source, string(effectiveSignal)).Inc()
	logCorrelationDecision(s, source, signal, correlationDecision)
	logObservedPayload(s, observed)
	if decodeError != "" {
		s.rememberObserved(observed)
		return observed, nil, false, nil
	}
	result, matched := s.observe(observed)
	if matched && result != nil && source == "exporter" {
		logComparisonSummary(s, *result)
	}
	return observed, result, matched, nil
}

func (s *Service) lookupTranslator(name string) (PayloadTranslator, error) { //nolint:ireturn
	if name == "" {
		name = defaultTranslatorID
	}
	s.translatorMu.RLock()
	translator, ok := s.translators[name]
	s.translatorMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown translator %q", name)
	}
	return translator, nil
}

func (s *Service) resolvePairForRequest(r *http.Request, source, listener string) configuredPair {
	requested := strings.TrimSpace(r.Header.Get(pairHeaderKey))

	s.pairMu.RLock()
	defer s.pairMu.RUnlock()

	if requested != "" {
		if pair, ok := s.pairs[requested]; ok {
			return pair
		}
		s.logger.Warn("unknown fidelity pair", zap.String("requested", requested), zap.String("default", s.defaultPair))
		return s.pairs[s.defaultPair]
	}

	var (
		pairID      string
		matchedPort string
	)
	portMap := s.receiverPairByPort
	if source == "exporter" {
		portMap = s.exporterPairByPort
	}
	for _, port := range requestPortCandidates(r, listener) {
		if id, ok := portMap[port]; ok {
			pairID = id
			matchedPort = port
			break
		}
	}
	if pairID != "" {
		if pair, ok := s.pairs[pairID]; ok {
			s.logger.Info("resolved fidelity pair from port", zap.String("source", source), zap.String("port", matchedPort), zap.String("pair", pairID))
			return pair
		}
		s.logger.Warn("port mapping matched unknown pair", zap.String("pair_id", pairID), zap.String("default", s.defaultPair))
	}
	return s.pairs[s.defaultPair]
}

func (s *Service) newConfiguredPair(cfg PairConfig) (configuredPair, error) {
	id := strings.TrimSpace(cfg.ID)
	if id == "" {
		return configuredPair{}, errors.New("pair id is required")
	}
	receiverTranslator := strings.TrimSpace(cfg.ReceiverTranslator)
	if receiverTranslator == "" {
		receiverTranslator = defaultTranslatorID
	}
	if _, err := s.lookupTranslator(receiverTranslator); err != nil {
		return configuredPair{}, fmt.Errorf("invalid receiver translator: %w", err)
	}
	exporterTranslator := strings.TrimSpace(cfg.ExporterTranslator)
	if exporterTranslator == "" {
		exporterTranslator = defaultTranslatorID
	}
	if _, err := s.lookupTranslator(exporterTranslator); err != nil {
		return configuredPair{}, fmt.Errorf("invalid exporter translator: %w", err)
	}
	receiverUpstream, err := parseOptionalURL(strings.TrimSpace(cfg.ReceiverUpstream))
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid receiver upstream: %w", err)
	}
	exporterUpstream, err := parseOptionalURL(strings.TrimSpace(cfg.ExporterUpstream))
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid exporter upstream: %w", err)
	}
	receiverPorts, err := normalizePortList(cfg.ReceiverPorts)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid receiver ports: %w", err)
	}
	exporterPorts, err := normalizePortList(cfg.ExporterPorts)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid exporter ports: %w", err)
	}

	return configuredPair{
		PairConfig: PairConfig{
			ID:                 id,
			ReceiverTranslator: receiverTranslator,
			ExporterTranslator: exporterTranslator,
			ReceiverUpstream:   strings.TrimSpace(cfg.ReceiverUpstream),
			ExporterUpstream:   strings.TrimSpace(cfg.ExporterUpstream),
			ReceiverPorts:      receiverPorts,
			ExporterPorts:      exporterPorts,
		},
		receiverUpstream: receiverUpstream,
		exporterUpstream: exporterUpstream,
	}, nil
}

func (s *Service) rebuildPortMappingsLocked() {
	s.receiverPairByPort = make(map[string]string)
	s.exporterPairByPort = make(map[string]string)
	for _, pair := range s.pairs {
		for _, port := range pair.ReceiverPorts {
			s.receiverPairByPort[port] = pair.ID
		}
		for _, port := range pair.ExporterPorts {
			s.exporterPairByPort[port] = pair.ID
		}
	}
}

func normalizePortList(rawPorts []string) ([]string, error) {
	if len(rawPorts) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(rawPorts))
	ports := make([]string, 0, len(rawPorts))
	for _, raw := range rawPorts {
		port, ok := canonicalPort(raw)
		if !ok {
			return nil, fmt.Errorf("bad port value %q", raw)
		}
		if _, exists := seen[port]; exists {
			continue
		}
		seen[port] = struct{}{}
		ports = append(ports, port)
	}
	slices.Sort(ports)
	return ports, nil
}

func requestPortCandidates(r *http.Request, listener string) []string {
	candidates := make([]string, 0, 5)
	appendCandidate := func(raw string) {
		if port, ok := canonicalPort(raw); ok {
			if slices.Contains(candidates, port) {
				return
			}
			candidates = append(candidates, port)
		}
	}

	appendCandidate(r.Header.Get("X-Forwarded-Port"))
	appendCandidate(r.Header.Get("X-Envoy-Original-Dst-Host"))
	appendCandidate(r.Header.Get("X-Forwarded-Host"))
	appendCandidate(r.Host)
	appendCandidate(listener)

	return candidates
}

func canonicalPort(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", false
	}
	if idx := strings.Index(value, ","); idx >= 0 {
		value = strings.TrimSpace(value[:idx])
	}
	value = strings.TrimPrefix(value, ":")
	if value == "" {
		return "", false
	}
	if _, err := strconv.Atoi(value); err == nil {
		return value, true
	}
	if host, port, err := net.SplitHostPort(value); err == nil {
		_ = host
		if _, err := strconv.Atoi(port); err == nil {
			return port, true
		}
	}
	return "", false
}

type correlationResolution struct {
	CorrelationID string
	Strategy      string
	Field         string
	RawValue      string
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Service) captureDatadogAPIRequest(listener string, r *http.Request) {
	body := mustReadBodyBytes(s, r)
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
		pair:        defaultPairID,
		translator:  defaultTranslatorID,
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

	logObservedPayload(s, observed)
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
		for key, val := range typed {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			flattenValue(result, next, val)
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
	correlationID, _, _, ok := deriveCorrelationFromFieldsDetailed(signal, fields)
	if !ok {
		return ""
	}
	return correlationID
}

func deriveCorrelationFromFieldsDetailed(signal Signal, fields map[string]string) (string, string, string, bool) {
	for _, key := range correlationCandidates(fields) {
		if candidate := correlationValueForField(key, fields[key]); candidate != "" {
			return string(signal) + ":" + candidate, key, fields[key], true
		}
	}
	return "", "", "", false
}

func resolveCorrelationIDFromDecoded(signal Signal, translatorCorrelationID string, fields map[string]string, headers http.Header, body []byte) correlationResolution {
	if translatorCorrelationID = strings.TrimSpace(translatorCorrelationID); translatorCorrelationID != "" {
		return correlationResolution{
			CorrelationID: string(signal) + ":" + translatorCorrelationID,
			Strategy:      "translator",
			Field:         "correlation_id",
			RawValue:      translatorCorrelationID,
		}
	}
	return resolveCorrelationID(signal, fields, headers, body)
}

func resolveCorrelationID(signal Signal, fields map[string]string, headers http.Header, body []byte) correlationResolution {
	if headerKey, headerValue := firstHeaderValue(headers, "X-Correlation-ID", "X-Fidelity-ID", "X-Request-ID"); headerValue != "" {
		return correlationResolution{
			CorrelationID: string(signal) + ":" + headerValue,
			Strategy:      "header",
			Field:         headerKey,
			RawValue:      headerValue,
		}
	}
	if correlationID, field, rawValue, ok := deriveCorrelationFromFieldsDetailed(signal, fields); ok {
		return correlationResolution{
			CorrelationID: correlationID,
			Strategy:      "field",
			Field:         field,
			RawValue:      rawValue,
		}
	}
	if len(fields) > 0 {
		return correlationResolution{
			CorrelationID: deriveFingerprintCorrelationID(signal, fields),
			Strategy:      "fingerprint",
		}
	}
	return correlationResolution{
		CorrelationID: deriveRawBodyCorrelationID(signal, body),
		Strategy:      "raw_body",
	}
}

func firstHeaderValue(headers http.Header, keys ...string) (string, string) {
	for _, key := range keys {
		if value := headers.Get(key); value != "" {
			return key, value
		}
	}
	return "", ""
}

func logCorrelationDecision(s *Service, source string, signal Signal, decision correlationResolution) {
	switch decision.Strategy {
	case "field", "header":
		s.logger.Info("correlation selection",
			zap.String("source", source),
			zap.String("signal", string(signal)),
			zap.String("strategy", decision.Strategy),
			zap.String("key", decision.Field),
			zap.String("raw_value", decision.RawValue),
			zap.String("correlation_id", decision.CorrelationID),
		)
	default:
		s.logger.Info("correlation selection",
			zap.String("source", source),
			zap.String("signal", string(signal)),
			zap.String("strategy", decision.Strategy),
			zap.String("correlation_id", decision.CorrelationID),
		)
	}
}

func deriveFingerprintCorrelationID(signal Signal, fields map[string]string) string {
	// Stable Identity Fields by signal type
	identityFields := map[Signal][]string{
		SignalTraces:  {"trace_id", "traceID", "[0][0].trace_id"},
		SignalMetrics: {"metric_name", "point_timestamp", "series[0].metric", "series[0].points[0][0]"},
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
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		for _, tag := range stableTags {
			for _, fieldKey := range keys {
				val := fields[fieldKey]
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

func parseExporterPath(rawPath string) (string, string) {
	path := strings.TrimSpace(rawPath)
	if path == "" {
		return "", "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	segments := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	if len(segments) < 2 {
		return "", path
	}

	var exporter string
	if segments[0] == "exporter" && len(segments) >= 2 {
		exporter = strings.ToLower(strings.TrimSpace(segments[1]))
		if len(segments) == 2 {
			return exporter, "/"
		}
		return exporter, "/" + strings.Join(segments[2:], "/")
	}

	switch segments[0] {
	case "api", "v0.2", "v0.3", "v0.4", "v0.5", "v1", "v2":
		return "", path
	default:
		exporter = strings.ToLower(strings.TrimSpace(segments[0]))
		return exporter, "/" + strings.Join(segments[1:], "/")
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
		payload, err := decodeDatadogSeriesProto(path, decoded)
		if err != nil {
			return nil, "", err
		}
		return payload, "protobuf", nil
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
		"correlation_id",
		"fidelity_correlation_id",
		"fidelity.correlation_id",
		"trace_id",
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
			strings.HasSuffix(lower, ".ddtags"),
			lower == "ddtags",
			strings.Contains(lower, ".tags[") && strings.Contains(valueLower, "correlation_id:"):
			add(key)
		default:
		}
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
		"X-Fidelity-ID",
		"X-Fidelity-Pair",
		"X-Request-ID",
		"Host",
		"X-Forwarded-Port",
		"X-Forwarded-Host",
		"X-Envoy-Original-Dst-Host",
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

func mustReadBodyBytes(s *Service, r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("failed to read request body", zap.Error(err))
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

func copyResponse(s *Service, w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		s.logger.Warn("failed to copy upstream response body", zap.Error(err))
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
		Pair:          payload.pair,
		Translator:    payload.translator,
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

func logObservedPayload(s *Service, payload *observedPayload) {
	debug := debugPayloadFromObserved(payload)
	body, err := json.Marshal(debug)
	if err != nil {
		s.logger.Error("captured payload marshal error",
			zap.String("source", payload.source),
			zap.String("signal", string(payload.signal)),
			zap.String("correlation_id", payload.correlation),
			zap.Error(err),
		)
		return
	}
	s.logger.Info("captured payload", zap.String("json", string(body)))
}

func logComparisonSummary(s *Service, result ComparisonResult) {
	requiredPassed := 0
	for _, check := range result.RequiredChecks {
		if check.Passed {
			requiredPassed++
		}
	}

	s.logger.Info("comparison result",
		zap.String("signal", string(result.Signal)),
		zap.String("correlation_id", result.CorrelationID),
		zap.Bool("policy_pass", result.Passed),
		zap.Bool("full_payload_pass", result.FullPayloadPassed),
		zap.Int("matched", len(result.Matched)),
		zap.Int("mismatched", len(result.Mismatched)),
		zap.Int("missing", len(result.MissingIn)),
		zap.Int("required_passed", requiredPassed),
		zap.Int("required_total", len(result.RequiredChecks)),
	)
}

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header       { return make(http.Header) }
func (noopResponseWriter) Write([]byte) (int, error) { return 0, nil }
func (noopResponseWriter) WriteHeader(int)           {}

func writeJSON(s *Service, w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
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
