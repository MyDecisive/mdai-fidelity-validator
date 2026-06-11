package validator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	defaultPairID       = "default"
	defaultTranslatorID = "datadog_raw"
	pairHeaderKey       = "X-Fidelity-Pair"
	configReloadEnvVar  = "MDAI_CONFIG_RELOAD_INTERVAL"
	connectionEnvVar    = "MDAI_CONNECTION_NAME"
	defaultConnection   = "default"
	observePrefix       = "/observe"
)

type PayloadTranslator interface {
	Name() string
	Decode(signal Signal, path, contentEncoding, contentType string, body []byte) DecodedPayload
}

// BatchDecoder is an optional extension of PayloadTranslator. When implemented, captureRequests
// calls DecodeAll instead of Decode, receiving one DecodedPayload per natural item group
// (one per trace group for traces, one per metric series for metrics). This lets a single
// HTTP batch that is later fan-out by the collector still match item-by-item on the exporter side.
type BatchDecoder interface {
	PayloadTranslator
	DecodeAll(signal Signal, path, contentEncoding, contentType string, body []byte) []DecodedPayload
}

type DecodedPayload struct {
	Signal        Signal
	CorrelationID string
	Attributes    map[string]string
	RawGroup      map[string]string // pre-mapping flat keys for this group (e.g. "[j].field")
	Spans         []map[string]string
	Format        string
	DecodeError   string
}

type datadogRawTranslator struct {
	mapping *mappingStore
}

func (datadogRawTranslator) Name() string { return defaultTranslatorID }

func (t datadogRawTranslator) Decode(signal Signal, path, contentEncoding, contentType string, body []byte) DecodedPayload {
	items := t.DecodeAll(signal, path, contentEncoding, contentType, body)
	if len(items) > 0 {
		return items[0]
	}
	return DecodedPayload{Signal: signal, Attributes: map[string]string{}, Format: "raw"}
}

// DecodeAll implements BatchDecoder. It decodes the body once, partitions the flat key map
// into per-trace-group or per-metric-series slices, applies field mapping to each slice
// independently, and returns one DecodedPayload per group. This lets a batch containing
// multiple traces or metric series be matched item-by-item against exporter payloads.
func (t datadogRawTranslator) DecodeAll(signal Signal, path, contentEncoding, contentType string, body []byte) []DecodedPayload {
	decodedBody, format, err := decodeBody(body, path, contentEncoding, contentType)
	if err != nil {
		return []DecodedPayload{{
			Signal:      signal,
			Attributes:  map[string]string{},
			Format:      "raw",
			DecodeError: fmt.Sprintf("failed to decode payload: %v", err),
		}}
	}
	canonicalSignal := signal
	if canonicalSignal == SignalUnknown {
		_, normalizedPath := parseExporterPath(path)
		canonicalSignal = inferSignalFromDatadogPath(normalizedPath)
	}

	rawFlat := flattenValueMap(decodedBody)
	var rawGroups []map[string]string
	switch canonicalSignal {
	case SignalTraces:
		// The Datadog Agent API for /v0.4/traces sends [[span,...],...]  — a root-level
		// array of trace groups with no "traces" wrapper. The Datadog intake API (e.g.
		// /api/v0.2/traces) wraps it as {"traces": [[span,...],...]}.
		// Try the wrapped form first; fall back to root-level grouping when no "traces[" key exists.
		hasWrappedTraces := false
		for key := range rawFlat {
			if strings.HasPrefix(key, "traces[") {
				hasWrappedTraces = true
				break
			}
		}
		if hasWrappedTraces {
			rawGroups = extractIndexedGroups(rawFlat, "traces")
		} else {
			rawGroups = extractIndexedGroups(rawFlat, "")
		}
	case SignalMetrics:
		rawGroups = extractIndexedGroups(rawFlat, "series")
	case SignalLogs:
		// Datadog log payloads are root-level JSON arrays: [{entry0}, {entry1}, ...].
		// Flat keys are [i].field. Group entries by their per-entry correlation_id field so
		// a collector-merged batch (entries from multiple receiver requests interleaved)
		// is split into one payload per original batch, while a same-source batch (all entries
		// sharing one correlation_id or with no per-entry id) stays together as one payload.
		rawGroups = t.groupLogsByCorrelation(canonicalSignal, path, rawFlat)
	default:
		rawGroups = []map[string]string{rawFlat}
	}

	out := make([]DecodedPayload, 0, len(rawGroups))
	for _, rawGroup := range rawGroups {
		var spans []map[string]string
		if canonicalSignal == SignalTraces {
			group := newTraceGroup(rawGroup)
			slices.SortStableFunc(group.spans, compareSpanIDs)
			spans = group.spans
			if len(group.spans) > 1 {
				rawGroup = group.flatten()
			}
		}
		fields := t.mapping.MapForPath(canonicalSignal, path, rawGroup)
		decodeError := ""
		if len(fields) == 0 {
			decodeError = "field mapping produced no canonical attributes"
		}
		dp := DecodedPayload{
			Signal:        canonicalSignal,
			CorrelationID: fields["correlation_id"],
			Attributes:    fields,
			Format:        format,
			DecodeError:   decodeError,
		}
		if canonicalSignal == SignalTraces {
			dp.RawGroup = rawGroup
			dp.Spans = spans
		}
		out = append(out, dp)
	}
	return out
}

// groupLogsByCorrelation partitions a flat log batch by per-entry correlation_id.
// Log payloads are root-level JSON arrays; raw keys are [i].field after flattening.
// When all entries share the same correlation_id (or have none), the original flat map is
// returned as-is so field mapping sees the full batch together. When entries from multiple
// originating batches are interleaved (different correlation_ids), they are regrouped and
// re-indexed so each group forms a coherent batch for independent matching.
func (t datadogRawTranslator) groupLogsByCorrelation(signal Signal, path string, rawFlat map[string]string) []map[string]string {
	entryGroups := extractIndexedGroups(rawFlat, "")
	if len(entryGroups) <= 1 {
		return []map[string]string{rawFlat}
	}

	type corrGroup struct{ entries []map[string]string }
	byCorr := make(map[string]*corrGroup, len(entryGroups))
	corrOrder := make([]string, 0, len(entryGroups))

	for _, entry := range entryGroups {
		fields := t.mapping.MapForPath(signal, path, entry)
		corrID := fields["correlation_id"] // empty string if the entry has no correlation_id
		if _, seen := byCorr[corrID]; !seen {
			byCorr[corrID] = &corrGroup{}
			corrOrder = append(corrOrder, corrID)
		}
		byCorr[corrID].entries = append(byCorr[corrID].entries, entry)
	}

	// All entries share one correlation_id (or all have none): keep the batch intact so
	// field mapping accessors like "[0].field" work correctly on the original indices.
	if len(byCorr) == 1 {
		return []map[string]string{rawFlat}
	}

	// Multiple correlation groups: build a re-indexed combined raw map per group.
	out := make([]map[string]string, 0, len(corrOrder))
	for _, corrID := range corrOrder {
		g := byCorr[corrID]
		combined := make(map[string]string, len(g.entries)*8)
		for i, entry := range g.entries {
			prefix := fmt.Sprintf("[%d].", i)
			for k, v := range entry {
				combined[prefix+k] = v
			}
		}
		out = append(out, combined)
	}
	return out
}

type PairConfig struct {
	ID                  string   `json:"id"`
	ReceiverTranslator  string   `json:"receiver_translator"`
	ExporterTranslator  string   `json:"exporter_translator"`
	ReceiverPorts       []string `json:"receiver_ports,omitempty"`
	ExporterPorts       []string `json:"exporter_ports,omitempty"`
	ReceiverIgnorePaths []string `json:"receiver_ignore_paths,omitempty"`
	ExporterIgnorePaths []string `json:"exporter_ignore_paths,omitempty"`
}

type configuredPair struct {
	PairConfig
}

type Service struct {
	logger     *zap.Logger
	retention  time.Duration
	connection string
	policy     Policy
	policyMu   sync.RWMutex

	stateMu     sync.RWMutex
	pending     map[string]*observedPayload
	lastResults map[string]ComparisonResult

	lastBySource sync.Map
	lastByKey    sync.Map
	pendingTotal atomic.Int64

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

	ingestAddr      string
	exporterAPIAddr string
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
	rawGroup    map[string]string // pre-mapping flat keys for this group
	spans       []map[string]string
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
	Spans             []SpanComparison         `json:"spans,omitempty"`
	ReceiverWire      RequestSnapshot          `json:"receiver_wire"`
	ExporterWire      RequestSnapshot          `json:"exporter_wire"`
	ComparedAt        time.Time                `json:"compared_at"`
}

type SpanComparison struct {
	SpanID     string         `json:"span_id"`
	Name       string         `json:"name,omitempty"`
	OnlyIn     string         `json:"only_in,omitempty"` // "receiver" or "exporter" for unmatched spans
	Matched    []string       `json:"matched,omitempty"`
	Mismatched []SpanDelta    `json:"mismatched,omitempty"`
	MissingIn  []MissingField `json:"missing_in,omitempty"`
	Passed     bool           `json:"passed"`
}

type SpanDelta struct {
	Attribute string `json:"attribute"`
	Receiver  string `json:"receiver"`
	Exporter  string `json:"exporter"`
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

func NewService(ctx context.Context, logger *zap.Logger, retention time.Duration, ingestAddr, exporterAPIAddr string) (*Service, error) {
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
		logger:      logger,
		retention:   retention,
		connection:  resolveConnectionName(),
		policy:      policy,
		pending:     make(map[string]*observedPayload),
		lastResults: make(map[string]ComparisonResult),
		defaultPair: defaultPairID,
		translators: map[string]PayloadTranslator{
			defaultTranslatorID: datadogRawTranslator{mapping: newMappingStore(fieldMap)},
		},
		pairs: map[string]configuredPair{
			defaultPairID: {
				PairConfig: PairConfig{
					ID:                 defaultPairID,
					ReceiverTranslator: defaultTranslatorID,
					ExporterTranslator: defaultTranslatorID,
					// Datadog agent housekeeping and aggregate paths with no
					// corresponding exporter-side payload or no supported decoder.
					ReceiverIgnorePaths: []string{
						"/api/v0.2/stats",
						"/api/v1/metadata",
						"/api/beta/sketches",
						"/support/flare",
						"/intake/",
					},
				},
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

		ingestAddr:      ingestAddr,
		exporterAPIAddr: exporterAPIAddr,
	}

	svc.startConfigReloader(ctx)
	svc.startMaintenanceLoops(ctx)
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
	mux.HandleFunc(observePrefix+"/receiver/", s.handleSource("receiver"))
	mux.HandleFunc(observePrefix+"/exporter/", s.handleSource("exporter"))
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

func (s *Service) startConfigReloader(ctx context.Context) {
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

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reload()
			}
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
