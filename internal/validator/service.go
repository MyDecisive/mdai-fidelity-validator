package validator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const numShards = 32

const (
	defaultPairID       = "default"
	defaultTranslatorID = "datadog_raw"
	pairHeaderKey       = "X-Fidelity-Pair"
	configReloadEnvVar  = "MDAI_CONFIG_RELOAD_INTERVAL"
	connectionEnvVar    = "MDAI_CONNECTION_NAME"
	defaultConnection   = "default"
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
		CorrelationID: fields["correlation_id"],
		Attributes:    fields,
		Format:        format,
		DecodeError:   decodeError,
	}
}

type PairConfig struct {
	ID                  string   `json:"id"`
	ReceiverTranslator  string   `json:"receiver_translator"`
	ExporterTranslator  string   `json:"exporter_translator"`
	ReceiverUpstream    string   `json:"receiver_upstream,omitempty"`
	ExporterUpstream    string   `json:"exporter_upstream,omitempty"`
	ReceiverPorts       []string `json:"receiver_ports,omitempty"`
	ExporterPorts       []string `json:"exporter_ports,omitempty"`
	ReceiverIgnorePaths []string `json:"receiver_ignore_paths,omitempty"`
	ExporterIgnorePaths []string `json:"exporter_ignore_paths,omitempty"`
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
	connection string
	httpClient *http.Client
	policy     Policy
	policyMu   sync.RWMutex

	lastBySource sync.Map
	lastByKey    sync.Map
	pendingTotal atomic.Int64

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
	pendingGauge  *prometheus.GaugeVec
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
		connection:       resolveConnectionName(),
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
			Help: "Number of payloads received by connection, source, and signal.",
		}, []string{"mdai_connection", "source", "signal"}),
		attributeEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_attribute_checks_total",
			Help: "Number of attribute comparisons by connection, signal, attribute, and result.",
		}, []string{"mdai_connection", "signal", "attribute", "result"}),
		signalEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_signal_checks_total",
			Help: "Number of whole-signal comparisons by connection, signal, and result.",
		}, []string{"mdai_connection", "signal", "result"}),
		requiredEval: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_required_attribute_checks_total",
			Help: "Number of required attribute comparisons by connection, signal, attribute, and result.",
		}, []string{"mdai_connection", "signal", "attribute", "result"}),
		requiredSig: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mdai_fidelity_required_signal_checks_total",
			Help: "Number of policy-based whole-signal comparisons by connection, signal, and result.",
		}, []string{"mdai_connection", "signal", "result"}),
		pendingGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mdai_fidelity_pending_payloads",
			Help: "Number of payloads waiting for their correlated counterpart by connection.",
		}, []string{"mdai_connection"}),
	}

	for i := range numShards {
		svc.shards[i] = &shard{
			pending:    make(map[string]*observedPayload),
			lastResult: make(map[string]ComparisonResult),
		}
	}
	svc.startConfigReloader()
	svc.startMaintenanceLoops()
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

func (*Service) MetricsRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
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

func resolveConnectionName() string {
	connectionName := strings.TrimSpace(os.Getenv(connectionEnvVar))
	if connectionName == "" {
		return defaultConnection
	}
	return connectionName
}
