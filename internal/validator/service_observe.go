package validator

import (
	"context"
	"time"
)

func (s *Service) observe(payload *observedPayload) (*ComparisonResult, bool) {
	s.rememberObserved(payload)

	existing, matched := s.extractPair(payload)
	if !matched {
		return nil, false
	}

	result := comparePair(existing, payload, s.currentPolicy())

	s.stateMu.Lock()
	existingResult, ok := s.lastResults[payload.correlation]
	if ok && existingResult.ComparedAt.After(result.ComparedAt) {
		s.stateMu.Unlock()
		return &existingResult, true
	}
	s.lastResults[payload.correlation] = result
	s.stateMu.Unlock()

	s.recordMetrics(result)

	return &result, true
}

func (s *Service) extractPair(payload *observedPayload) (*observedPayload, bool) {
	now := time.Now().UTC()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if existing, ok := s.pending[payload.correlation]; ok {
		if now.Sub(existing.receivedAt) <= s.retention {
			if existing.source == payload.source {
				s.pending[payload.correlation] = payload
				return nil, false
			}
			delete(s.pending, payload.correlation)
			s.adjustPendingTotal(-1)
			return existing, true
		}
		delete(s.pending, payload.correlation)
		s.adjustPendingTotal(-1)
	}

	s.pending[payload.correlation] = payload
	s.adjustPendingTotal(1)
	return nil, false
}

func (s *Service) rememberObserved(payload *observedPayload) {
	s.lastBySource.Store(payload.source, payload)
	s.lastByKey.Store(payload.source+":"+string(payload.signal), payload)
}

func (s *Service) adjustPendingTotal(delta int64) {
	total := s.pendingTotal.Add(delta)
	if total < 0 {
		s.pendingTotal.CompareAndSwap(total, 0)
		total = 0
	}
	s.pendingGauge.WithLabelValues(s.connection).Set(float64(total))
}

func (s *Service) startMaintenanceLoops(ctx context.Context) {
	interval := stateGCInterval(s.retention)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.gcExpiredState(now.UTC())
			}
		}
	}()
}

func (s *Service) gcExpiredState(now time.Time) {
	s.stateMu.Lock()
	for key, payload := range s.pending {
		if now.Sub(payload.receivedAt) > s.retention {
			delete(s.pending, key)
		}
	}
	for key, result := range s.lastResults {
		if now.Sub(result.ComparedAt) > s.retention {
			delete(s.lastResults, key)
		}
	}
	pending := int64(len(s.pending))
	s.stateMu.Unlock()

	s.pendingTotal.Store(pending)
	s.pendingGauge.WithLabelValues(s.connection).Set(float64(pending))
}

func stateGCInterval(retention time.Duration) time.Duration {
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
