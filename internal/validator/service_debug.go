package validator

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"go.uber.org/zap"
)

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

func summarizePolicy(policy Policy) string {
	signals := make([]string, 0, len(policy.Signals))
	for signal, signalPolicy := range policy.Signals {
		signals = append(signals, fmt.Sprintf("%s:%d", signal, len(signalPolicy.RequiredAttributes)))
	}
	slices.Sort(signals)
	return strings.Join(signals, ",")
}
