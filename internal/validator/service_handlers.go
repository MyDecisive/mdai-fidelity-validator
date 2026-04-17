package validator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"go.uber.org/zap"
)

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

	raw, ok := s.lastBySource.Load(source)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	payload, ok := raw.(*observedPayload)
	if !ok || payload == nil {
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

	raw, ok := s.lastByKey.Load(parts[0] + ":" + parts[1])
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	payload, ok := raw.(*observedPayload)
	if !ok || payload == nil {
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
	pair := s.resolvePairForRequest(r, "exporter", ":18081")
	if pair.shouldIgnorePath("exporter", path) {
		s.logger.Info("ignoring exporter payload", zap.String("pair", pair.ID), zap.String("path", normalizedPath))
		body := mustReadBodyBytes(s, r)
		upstream := pair.exporterUpstream
		if upstream == nil {
			upstream = s.exporterUpstream
		}
		if upstream == nil {
			writeDatadogAck(w, signal)
			return
		}

		resp, err := s.forwardRaw(r.Context(), upstream, normalizedPath, r.URL.RawQuery, r.Method, r.Header, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close() //nolint:errcheck

		copyResponse(s, w, resp)
		return
	}

	s.handleCommonIngest(w, r, "exporter", signal, ":18081", path, normalizedPath)
}

func (s *Service) handleProxyIngest(w http.ResponseWriter, r *http.Request) {
	path := readRequestPath(r)
	signal := inferSignalFromDatadogPath(path)
	pair := s.resolvePairForRequest(r, "receiver", ":8126")
	if pair.shouldIgnorePath("receiver", path) {
		s.logger.Info("ignoring receiver payload", zap.String("pair", pair.ID), zap.String("path", path))
		body := mustReadBodyBytes(s, r)
		upstream := pair.receiverUpstream
		if upstream == nil {
			upstream = s.receiverUpstream
		}
		if upstream == nil {
			writeDatadogAck(w, signal)
			return
		}

		resp, err := s.forwardRaw(r.Context(), upstream, path, r.URL.RawQuery, r.Method, r.Header, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("forwarding failed: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close() //nolint:errcheck

		copyResponse(s, w, resp)
		return
	}

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
