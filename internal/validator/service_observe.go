package validator

import (
	"github.com/cespare/xxhash/v2"
	"time"
)

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
	s.pendingGauge.WithLabelValues(s.connection).Set(float64(total))
}

func (s *Service) recordMetrics(result ComparisonResult) {
	for _, attribute := range result.Matched {
		s.attributeEval.WithLabelValues(s.connection, string(result.Signal), attribute, "pass").Inc()
	}
	for _, delta := range result.Mismatched {
		s.attributeEval.WithLabelValues(s.connection, string(result.Signal), delta.Attribute, "fail").Inc()
	}
	for _, missing := range result.MissingIn {
		s.attributeEval.WithLabelValues(s.connection, string(result.Signal), missing.Attribute, "fail").Inc()
	}

	if result.FullPayloadPassed {
		s.signalEval.WithLabelValues(s.connection, string(result.Signal), "pass").Inc()
	} else {
		s.signalEval.WithLabelValues(s.connection, string(result.Signal), "fail").Inc()
	}

	if result.Passed {
		s.requiredSig.WithLabelValues(s.connection, string(result.Signal), "pass").Inc()
	} else {
		s.requiredSig.WithLabelValues(s.connection, string(result.Signal), "fail").Inc()
	}
	for _, check := range result.RequiredChecks {
		resultLabel := "fail"
		if check.Passed {
			resultLabel = "pass"
		}
		s.requiredEval.WithLabelValues(s.connection, string(result.Signal), check.Attribute, resultLabel).Inc()
	}
}
