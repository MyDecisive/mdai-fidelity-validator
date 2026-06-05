package validator

import (
	"errors"
	"fmt"
	"strings"

	ddmetrics "github.com/DataDog/agent-payload/v5/gogen"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	datadogMetricTypeCount  = 1
	datadogMetricTypeRate   = 2
	datadogMetricTypeGauge  = 3
	datadogMetricTypeSketch = 4
)

func decodeDatadogSeriesProto(path string, body []byte) (any, error) {
	_, normalizedPath := parseExporterPath(path)
	if !strings.HasSuffix(normalizedPath, "/series") {
		return nil, fmt.Errorf("protobuf payloads are not supported for path %q", normalizedPath)
	}

	var payload ddmetrics.MetricPayload
	if err := gogoproto.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal metric payload: %w", err)
	}
	if len(payload.GetSeries()) == 0 {
		return nil, errors.New("metric payload had no series")
	}
	series := make([]any, 0, len(payload.GetSeries()))
	for _, s := range payload.GetSeries() {
		series = append(series, metricSeriesToMap(s))
	}
	return map[string]any{"series": series}, nil
}

func metricSeriesToMap(s *ddmetrics.MetricPayload_MetricSeries) map[string]any {
	row := map[string]any{}
	if v := s.GetMetric(); v != "" {
		row["metric"] = v
	}
	if tags := s.GetTags(); len(tags) > 0 {
		t := make([]any, len(tags))
		for i, tag := range tags {
			t[i] = tag
		}
		row["tags"] = t
	}
	if resources := s.GetResources(); len(resources) > 0 {
		r := make([]any, len(resources))
		for i, res := range resources {
			rm := map[string]any{}
			if v := res.GetType(); v != "" {
				rm["type"] = v
			}
			if v := res.GetName(); v != "" {
				rm["name"] = v
			}
			r[i] = rm
		}
		row["resources"] = r
	}
	if points := s.GetPoints(); len(points) > 0 {
		p := make([]any, len(points))
		for i, pt := range points {
			p[i] = []any{float64(pt.GetTimestamp()), pt.GetValue()}
		}
		row["points"] = p
	}
	if t := datadogMetricTypeName(int(s.GetType())); t != "" { //nolint:gosec // enum value, bounded
		row["type"] = t
	}
	if v := s.GetUnit(); v != "" {
		row["unit"] = v
	}
	if v := s.GetSourceTypeName(); v != "" {
		row["source_type_name"] = v
	}
	if v := s.GetInterval(); v != 0 {
		row["interval"] = float64(v)
	}
	if originMap := buildOriginMap(s.GetMetadata()); len(originMap) > 0 {
		row["metadata"] = map[string]any{"origin": originMap}
	}
	return row
}

func buildOriginMap(meta *ddmetrics.Metadata) map[string]any {
	if meta == nil {
		return nil
	}
	origin := meta.GetOrigin()
	if origin == nil {
		return nil
	}
	out := map[string]any{}
	if v := origin.GetOriginProduct(); v != 0 {
		out["origin_product"] = float64(v)
	}
	if v := origin.GetOriginCategory(); v != 0 {
		out["origin_category"] = float64(v)
	}
	if v := origin.GetOriginService(); v != 0 {
		out["origin_service"] = float64(v)
	}
	return out
}

func datadogMetricTypeName(value int) string {
	switch value {
	case datadogMetricTypeCount:
		return "count"
	case datadogMetricTypeRate:
		return "rate"
	case datadogMetricTypeGauge:
		return "gauge"
	case datadogMetricTypeSketch:
		return "sketch"
	default:
		return ""
	}
}
