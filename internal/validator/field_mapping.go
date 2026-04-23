package validator

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	fieldMappingPathEnvVar = "MDAI_FIDELITY_FIELD_MAPPING_PATH"
)

type FieldMapping struct {
	Signals   map[Signal]map[string][]string   `yaml:"signals"`
	Exporters map[string]FieldMappingExporters `yaml:"exporters"`
}

type FieldMappingExporters struct {
	Signals map[Signal]map[string][]string `yaml:"signals"`
}

func loadFieldMapping() (FieldMapping, string, error) {
	mapping := defaultFieldMapping()

	configuredPath := strings.TrimSpace(os.Getenv(fieldMappingPathEnvVar))
	if configuredPath != "" {
		body, err := os.ReadFile(configuredPath) //nolint:gosec
		if err != nil {
			return FieldMapping{}, "", err
		}
		if err := yaml.Unmarshal(body, &mapping); err != nil {
			return FieldMapping{}, "", err
		}
		mapping = normalizeFieldMapping(mapping)
		return mapping, fmt.Sprintf("file:%s (via %s)", configuredPath, fieldMappingPathEnvVar), nil
	}

	mapping = normalizeFieldMapping(mapping)
	return mapping, "runtime-empty-defaults", nil
}

func defaultFieldMapping() FieldMapping {
	return FieldMapping{
		Signals:   map[Signal]map[string][]string{},
		Exporters: map[string]FieldMappingExporters{},
	}
}

func normalizeFieldMapping(input FieldMapping) FieldMapping {
	if input.Signals == nil {
		input.Signals = map[Signal]map[string][]string{}
	}
	if input.Exporters == nil {
		input.Exporters = map[string]FieldMappingExporters{}
		return input
	}

	normalized := make(map[string]FieldMappingExporters, len(input.Exporters))
	for exporter, profile := range input.Exporters {
		normalized[strings.ToLower(strings.TrimSpace(exporter))] = profile
	}
	input.Exporters = normalized
	return input
}

func (m FieldMapping) Map(signal Signal, fields map[string]string) map[string]string {
	return m.mapSignal(signal, m.Signals, fields)
}

func (m FieldMapping) MapForPath(signal Signal, path string, fields map[string]string) map[string]string {
	exporter, _ := parseExporterPath(path)
	if exporter != "" {
		if profile, ok := m.Exporters[strings.ToLower(exporter)]; ok {
			return m.mapBySignal(signal, mergeSignalMappings(m.Signals, profile.Signals), fields)
		}
	}
	return m.mapBySignal(signal, m.Signals, fields)
}

func mergeSignalMappings(base, override map[Signal]map[string][]string) map[Signal]map[string][]string {
	if len(base) == 0 && len(override) == 0 {
		return map[Signal]map[string][]string{}
	}

	merged := make(map[Signal]map[string][]string, len(base)+len(override))
	for signal, mappings := range base {
		merged[signal] = cloneCanonicalMappings(mappings)
	}
	for signal, mappings := range override {
		existing, ok := merged[signal]
		if !ok {
			merged[signal] = cloneCanonicalMappings(mappings)
			continue
		}
		for canonical, sources := range mappings {
			existing[canonical] = append([]string(nil), sources...)
		}
	}
	return merged
}

func cloneCanonicalMappings(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return map[string][]string{}
	}

	cloned := make(map[string][]string, len(input))
	for canonical, sources := range input {
		cloned[canonical] = append([]string(nil), sources...)
	}
	return cloned
}

func (m FieldMapping) mapBySignal(signal Signal, signalMappings map[Signal]map[string][]string, fields map[string]string) map[string]string {
	if signal != SignalUnknown {
		return m.mapSignal(signal, signalMappings, fields)
	}

	best := map[string]string{}
	for _, candidate := range []Signal{SignalLogs, SignalMetrics, SignalTraces} {
		mapped := m.mapSignal(candidate, signalMappings, fields)
		if len(mapped) > len(best) {
			best = mapped
		}
	}
	return best
}

func (FieldMapping) mapSignal(signal Signal, signalMappings map[Signal]map[string][]string, fields map[string]string) map[string]string {
	if signalMappings == nil {
		return map[string]string{}
	}

	perSignal, ok := signalMappings[signal]
	if !ok || len(perSignal) == 0 {
		return map[string]string{}
	}

	keys := make([]string, 0, len(perSignal))
	for key := range perSignal {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out := make(map[string]string, len(perSignal))
	for _, canonical := range keys {
		sources := perSignal[canonical]
		for _, source := range sources {
			if value, ok := extractMappedValue(fields, source); ok {
				out[canonical] = value
				break
			}
		}
	}

	return out
}
