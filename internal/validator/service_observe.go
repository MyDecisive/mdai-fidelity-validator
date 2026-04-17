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
	now := time.Now().UTC()
	sh := s.getShard(payload.correlation)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	s.rememberObserved(payload)

	if existing, ok := sh.pending[payload.correlation]; ok {
		if now.Sub(existing.receivedAt) > s.retention {
			delete(sh.pending, payload.correlation)
			s.adjustPendingTotal(-1)
		} else {
			if existing.source == payload.source {
				sh.pending[payload.correlation] = payload
				return nil, false
			}

			delete(sh.pending, payload.correlation)
			s.adjustPendingTotal(-1)

			result := comparePair(existing, payload, s.currentPolicy())
			sh.lastResult[payload.correlation] = result
			s.recordMetrics(result)
			return &result, true
		}
	}

	sh.pending[payload.correlation] = payload
	s.adjustPendingTotal(1)
	return nil, false
}

func (s *Service) rememberObserved(payload *observedPayload) {
	s.lastBySource.Store(payload.source, payload)
	s.lastByKey.Store(payload.source+":"+string(payload.signal), payload)
}

func (s *Service) gcShardLocked(sh *shard, now time.Time) int {
	expiredPending := 0
	for key, payload := range sh.pending {
		if now.Sub(payload.receivedAt) > s.retention {
			delete(sh.pending, key)
			expiredPending++
		}
	}
	for key, result := range sh.lastResult {
		if now.Sub(result.ComparedAt) > s.retention {
			delete(sh.lastResult, key)
		}
	}
	return expiredPending
}

func (s *Service) adjustPendingTotal(delta int64) {
	total := s.pendingTotal.Add(delta)
	if total < 0 {
		s.pendingTotal.Store(0)
		total = 0
	}
	s.pendingGauge.WithLabelValues(s.connection).Set(float64(total))
}

func (s *Service) startMaintenanceLoops() {
	interval := shardGCInterval(s.retention)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for now := range ticker.C {
			s.gcExpiredShardState(now.UTC())
		}
	}()
}

func (s *Service) gcExpiredShardState(now time.Time) {
	expiredPending := int64(0)
	for _, sh := range s.shards {
		sh.mu.Lock()
		expiredPending += int64(s.gcShardLocked(sh, now))
		sh.mu.Unlock()
	}
	if expiredPending > 0 {
		s.adjustPendingTotal(-expiredPending)
	}
}

func shardGCInterval(retention time.Duration) time.Duration {
	switch {
	case retention <= 0:
		return 5 * time.Second
	case retention <= 4*time.Second:
		return time.Second
	default:
		interval := retention / 4
		if interval < time.Second {
			return time.Second
		}
		if interval > 30*time.Second {
			return 30 * time.Second
		}
		return interval
	}
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
