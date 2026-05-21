package validator

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

func (s *Service) captureRequest(pairID, translatorID, source string, signal Signal, listener, requestPath string, r *http.Request) (*observedPayload, *ComparisonResult, bool, error) {
	body, err := io.ReadAll(http.MaxBytesReader(noopResponseWriter{}, r.Body, 10<<20))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	translator, err := s.lookupTranslator(translatorID)
	if err != nil {
		return nil, nil, false, err
	}
	decoded := translator.Decode(signal, requestPath, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type"), body)
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
			Path:            requestPath,
			Query:           r.URL.RawQuery,
			ContentType:     r.Header.Get("Content-Type"),
			ContentEncoding: r.Header.Get("Content-Encoding"),
			Format:          format,
			DecodeError:     decodeError,
			Headers:         selectedHeaders(r.Header),
		},
		flattened: fields,
	}

	s.receivedTotal.WithLabelValues(s.connection, source, string(effectiveSignal)).Inc()
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
	receiverPorts, err := normalizePortList(cfg.ReceiverPorts)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid receiver ports: %w", err)
	}
	exporterPorts, err := normalizePortList(cfg.ExporterPorts)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid exporter ports: %w", err)
	}
	receiverIgnorePaths, err := normalizePathPatternList(cfg.ReceiverIgnorePaths)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid receiver ignore paths: %w", err)
	}
	exporterIgnorePaths, err := normalizePathPatternList(cfg.ExporterIgnorePaths)
	if err != nil {
		return configuredPair{}, fmt.Errorf("invalid exporter ignore paths: %w", err)
	}

	return configuredPair{
		PairConfig: PairConfig{
			ID:                  id,
			ReceiverTranslator:  receiverTranslator,
			ExporterTranslator:  exporterTranslator,
			ReceiverPorts:       receiverPorts,
			ExporterPorts:       exporterPorts,
			ReceiverIgnorePaths: receiverIgnorePaths,
			ExporterIgnorePaths: exporterIgnorePaths,
		},
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

func normalizePathPatternList(rawPatterns []string) ([]string, error) {
	if len(rawPatterns) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(rawPatterns))
	patterns := make([]string, 0, len(rawPatterns))
	for _, raw := range rawPatterns {
		pattern := strings.TrimSpace(raw)
		if pattern == "" {
			continue
		}
		if !strings.HasPrefix(pattern, "/") {
			pattern = "/" + pattern
		}
		if strings.ContainsAny(pattern, "*?[") {
			if _, err := path.Match(pattern, "/"); err != nil {
				return nil, fmt.Errorf("bad path pattern %q: %w", raw, err)
			}
		}
		if _, exists := seen[pattern]; exists {
			continue
		}
		seen[pattern] = struct{}{}
		patterns = append(patterns, pattern)
	}
	slices.Sort(patterns)
	return patterns, nil
}

func (p configuredPair) shouldIgnorePath(source, rawPath string) bool {
	var (
		patterns       []string
		candidatePaths []string
	)

	switch source {
	case "receiver":
		patterns = p.ReceiverIgnorePaths
		candidatePaths = []string{rawPath}
	case "exporter":
		patterns = p.ExporterIgnorePaths
		_, normalizedPath := parseExporterPath(rawPath)
		candidatePaths = []string{normalizedPath, rawPath}
	default:
		return false
	}

	for _, pattern := range patterns {
		for _, candidate := range candidatePaths {
			if candidate == "" {
				continue
			}
			matched, err := path.Match(pattern, candidate)
			if err == nil && matched {
				return true
			}
		}
	}
	return false
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

func (s *Service) captureDatadogAPIRequest(listener string, r *http.Request) {
	body := mustReadBodyBytes(s, r)
	requestPath := readRequestPath(r)
	signal := inferDatadogAPISignal(requestPath)
	format := "raw"
	fields := map[string]string{}
	decodeError := ""

	if len(body) > 0 {
		if decodedBody, decodedFormat, err := decodeBody(body, requestPath, r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type")); err == nil {
			fields = flattenValueMap(decodedBody)
			format = decodedFormat
		} else {
			decodeError = fmt.Sprintf("failed to decode payload: %v", err)
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
		decodeError: decodeError,
		request: RequestSnapshot{
			Listener:        listener,
			Method:          r.Method,
			Path:            requestPath,
			Query:           r.URL.RawQuery,
			ContentType:     r.Header.Get("Content-Type"),
			ContentEncoding: r.Header.Get("Content-Encoding"),
			Format:          format,
			DecodeError:     decodeError,
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
	case json.Number:
		result[prefix] = typed.String()
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

func inferSignalFromDatadogPath(requestPath string) Signal {
	switch {
	case strings.HasSuffix(requestPath, "/traces"):
		return SignalTraces
	case strings.HasSuffix(requestPath, "/series"), strings.HasSuffix(requestPath, "/check_run"), strings.HasSuffix(requestPath, "/sketches"), strings.HasSuffix(requestPath, "/distribution_points"):
		return SignalMetrics
	case strings.HasSuffix(requestPath, "/logs"):
		return SignalLogs
	default:
		return SignalUnknown
	}
}

func parseExporterPath(rawPath string) (string, string) {
	normalizedPath := strings.TrimSpace(rawPath)
	if normalizedPath == "" {
		return "", "/"
	}
	if !strings.HasPrefix(normalizedPath, "/") {
		normalizedPath = "/" + normalizedPath
	}

	segments := strings.FieldsFunc(normalizedPath, func(r rune) bool { return r == '/' })
	if len(segments) < 2 {
		return "", normalizedPath
	}

	var exporter string
	if segments[0] == "exporter" && len(segments) >= 2 {
		exporter = strings.ToLower(strings.TrimSpace(segments[1]))
		if len(segments) == 2 {
			return exporter, "/"
		}
		return exporter, "/" + strings.Join(segments[2:], "/")
	}

	if segments[0] == strings.TrimPrefix(observePrefix, "/") && len(segments) >= 3 && segments[1] == "exporter" {
		for i := 2; i < len(segments); i++ {
			if isDatadogAPIPrefixSegment(segments[i]) {
				if i == 2 {
					return "", "/" + strings.Join(segments[i:], "/")
				}
				exporter = strings.ToLower(strings.TrimSpace(segments[i-1]))
				return exporter, "/" + strings.Join(segments[i:], "/")
			}
		}
		return "", normalizedPath
	}

	switch segments[0] {
	case "api", "v0.2", "v0.3", "v0.4", "v0.5", "v1", "v2":
		return "", normalizedPath
	default:
		exporter = strings.ToLower(strings.TrimSpace(segments[0]))
		return exporter, "/" + strings.Join(segments[1:], "/")
	}
}

func isDatadogAPIPrefixSegment(segment string) bool {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "api", "v0.2", "v0.3", "v0.4", "v0.5", "v1", "v2":
		return true
	default:
		return false
	}
}

func inferDatadogAPISignal(requestPath string) Signal {
	switch requestPath {
	case "/api/v1/validate":
		return SignalValidate
	default:
		return SignalAPI
	}
}

func decodeBody(body []byte, requestPath, contentEncoding, contentType string) (any, string, error) {
	var decoded []byte
	format := "json"

	if decodedBody, err := decodeCompression(body, contentEncoding); err == nil {
		decoded = decodedBody
	} else {
		return nil, "", err
	}

	if strings.Contains(strings.ToLower(contentType), "protobuf") {
		payload, err := decodeDatadogProtoByPath(requestPath, decoded)
		if err != nil {
			return nil, "", err
		}
		return payload, "protobuf", nil
	}

	if looksLikeJSON(decoded) || strings.Contains(strings.ToLower(contentType), "json") {
		var payload any
		decoder := json.NewDecoder(bytes.NewReader(decoded))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err == nil {
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
	case "zstd":
		reader, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer reader.Close() //nolint:errcheck
		return io.ReadAll(reader)
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
