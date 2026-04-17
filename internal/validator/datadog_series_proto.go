package validator

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	datadogMetricTypeCount  = 1
	datadogMetricTypeRate   = 2
	datadogMetricTypeGauge  = 3
	datadogMetricTypeSketch = 4

	datadogValueTypeZero    = 0x00
	datadogValueTypeSint64  = 0x10
	datadogValueTypeFloat32 = 0x20
	datadogValueTypeFloat64 = 0x30
)

func decodeDatadogSeriesProto(path string, body []byte) (any, error) {
	_, normalizedPath := parseExporterPath(path)
	if !strings.HasSuffix(normalizedPath, "/series") {
		return nil, fmt.Errorf("protobuf payloads are not supported for path %q", normalizedPath)
	}

	v3Payload, v3Err := decodeDatadogSeriesProtoV3(body)
	if v3Err == nil {
		return v3Payload, nil
	}

	v2Payload, v2Err := decodeDatadogSeriesProtoV2(body)
	if v2Err == nil {
		return v2Payload, nil
	}

	return nil, fmt.Errorf(
		"failed to decode Datadog series protobuf payload: %w",
		errors.Join(fmt.Errorf("v3: %w", v3Err), fmt.Errorf("v2: %w", v2Err)),
	)
}

func decodeDatadogSeriesProtoV3(body []byte) (map[string]any, error) {
	var (
		metricDataRaw []byte
		metadata      map[string]any
	)

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 2:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, fmt.Errorf("consume v3 metadata: %w", err)
			}
			body = next
			parsed, err := parseDatadogSeriesV3Metadata(raw)
			if err != nil {
				return nil, err
			}
			if len(parsed) > 0 {
				metadata = parsed
			}
		case 3:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, fmt.Errorf("consume v3 metric data: %w", err)
			}
			body = next
			metricDataRaw = raw
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	if len(metricDataRaw) == 0 {
		return nil, errors.New("missing metricData field")
	}

	payload, err := parseDatadogSeriesV3MetricData(metricDataRaw)
	if err != nil {
		return nil, err
	}
	if metadata != nil {
		payload["metadata"] = metadata
	}
	return payload, nil
}

func parseDatadogSeriesV3Metadata(body []byte) (map[string]any, error) {
	var (
		tags      []any
		resources []any
	)

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			tags = append(tags, value)
		case 2:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			resources = append(resources, value)
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	out := map[string]any{}
	if len(tags) > 0 {
		out["tags"] = tags
	}
	if len(resources) > 0 {
		out["resources"] = resources
	}
	return out, nil
}

func parseDatadogSeriesV3MetricData(body []byte) (map[string]any, error) {
	raw, err := parseDatadogSeriesV3MetricDataRaw(body)
	if err != nil {
		return nil, err
	}
	decoded, err := raw.decode()
	if err != nil {
		return nil, err
	}
	return decoded.buildPayload()
}

func decodeDatadogSeriesProtoV2(body []byte) (map[string]any, error) {
	series := make([]any, 0)

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			row, err := parseDatadogSeriesProtoV2Series(raw)
			if err != nil {
				return nil, err
			}
			series = append(series, row)
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	if len(series) == 0 {
		return nil, errors.New("metric payload had no series")
	}
	return map[string]any{"series": series}, nil
}

func parseDatadogSeriesProtoV2Series(body []byte) (map[string]any, error) {
	row := map[string]any{}
	collector := &datadogSeriesV2Collector{}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		next, err := collector.consumeField(row, num, typ, body)
		if err != nil {
			return nil, err
		}
		body = next
	}

	collector.finalize(row)
	return row, nil
}

type datadogSeriesV3Raw struct {
	dictNameStrRaw      []byte
	dictTagStrRaw       []byte
	dictTagsetsRaw      []int64
	dictResourceStrRaw  []byte
	dictResourceLenRaw  []int64
	dictResourceTypeRaw []int64
	dictResourceNameRaw []int64
	dictSourceTypeRaw   []byte
	dictOriginInfoRaw   []int64
	dictUnitStrRaw      []byte
	typesRaw            []uint64
	nameRefsRaw         []int64
	tagsetRefsRaw       []int64
	resourcesRefsRaw    []int64
	intervalsRaw        []uint64
	numPointsRaw        []uint64
	sourceTypeNameRefs  []int64
	originInfoRefsRaw   []int64
	unitRefsRaw         []int64
	timestampsRaw       []int64
	valsSint64Raw       []int64
	valsFloat32Raw      []float64
	valsFloat64Raw      []float64
}

type datadogSeriesV3Decoded struct {
	names           []string
	tagSets         [][]string
	resourceSets    []any
	sourceTypeNames []string
	originInfo      []map[string]any
	unitStrings     []string
	typesRaw        []uint64
	nameRefs        []int
	tagsetRefs      []int
	resourceRefs    []int
	intervalsRaw    []uint64
	numPointsRaw    []uint64
	sourceRefs      []int
	originRefs      []int
	unitRefs        []int
	timestamps      []int64
	valsSint64Raw   []int64
	valsFloat32Raw  []float64
	valsFloat64Raw  []float64
}

type datadogSeriesV3PointCursor struct {
	timestampIndex int
	sintIndex      int
	float32Index   int
	float64Index   int
}

type datadogSeriesV2Collector struct {
	tags      []any
	points    []any
	resources []any
	metadata  map[string]any
}

func parseDatadogSeriesV3MetricDataRaw(body []byte) (datadogSeriesV3Raw, error) {
	raw := datadogSeriesV3Raw{}
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return datadogSeriesV3Raw{}, protowire.ParseError(n)
		}
		body = body[n:]

		next, err := raw.consumeField(num, typ, body)
		if err != nil {
			return datadogSeriesV3Raw{}, err
		}
		body = next
	}
	return raw, nil
}

func (raw *datadogSeriesV3Raw) consumeField(num protowire.Number, typ protowire.Type, body []byte) ([]byte, error) {
	switch num {
	case 1:
		return consumeV3Bytes(body, typ, &raw.dictNameStrRaw)
	case 2:
		return consumeV3Bytes(body, typ, &raw.dictTagStrRaw)
	case 3:
		return consumeV3PackedSint64(body, typ, &raw.dictTagsetsRaw)
	case 4:
		return consumeV3Bytes(body, typ, &raw.dictResourceStrRaw)
	case 5:
		return consumeV3PackedInt64(body, typ, &raw.dictResourceLenRaw)
	case 6:
		return consumeV3PackedSint64(body, typ, &raw.dictResourceTypeRaw)
	case 7:
		return consumeV3PackedSint64(body, typ, &raw.dictResourceNameRaw)
	case 8:
		return consumeV3Bytes(body, typ, &raw.dictSourceTypeRaw)
	case 9:
		return consumeV3PackedInt32(body, typ, &raw.dictOriginInfoRaw)
	case 10:
		return consumeV3PackedUint64(body, typ, &raw.typesRaw)
	case 11:
		return consumeV3PackedSint64(body, typ, &raw.nameRefsRaw)
	case 12:
		return consumeV3PackedSint64(body, typ, &raw.tagsetRefsRaw)
	case 13:
		return consumeV3PackedSint64(body, typ, &raw.resourcesRefsRaw)
	case 14:
		return consumeV3PackedUint64(body, typ, &raw.intervalsRaw)
	case 15:
		return consumeV3PackedUint64(body, typ, &raw.numPointsRaw)
	case 16:
		return consumeV3PackedSint64(body, typ, &raw.timestampsRaw)
	case 17:
		return consumeV3PackedSint64(body, typ, &raw.valsSint64Raw)
	case 18:
		return consumeV3PackedFloat32(body, typ, &raw.valsFloat32Raw)
	case 19:
		return consumeV3PackedFloat64(body, typ, &raw.valsFloat64Raw)
	case 23:
		return consumeV3PackedSint64(body, typ, &raw.sourceTypeNameRefs)
	case 24:
		return consumeV3PackedSint64(body, typ, &raw.originInfoRefsRaw)
	case 25:
		return consumeV3Bytes(body, typ, &raw.dictUnitStrRaw)
	case 26:
		return consumeV3PackedSint64(body, typ, &raw.unitRefsRaw)
	default:
		return skipFieldValue(num, typ, body)
	}
}

func (raw *datadogSeriesV3Raw) decode() (datadogSeriesV3Decoded, error) {
	names, err := parseDatadogStringTable(raw.dictNameStrRaw)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse name dictionary: %w", err)
	}
	tagStrings, err := parseDatadogStringTable(raw.dictTagStrRaw)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse tag dictionary: %w", err)
	}
	resourceStrings, err := parseDatadogStringTable(raw.dictResourceStrRaw)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse resource dictionary: %w", err)
	}
	sourceTypeNames, err := parseDatadogStringTable(raw.dictSourceTypeRaw)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse source type dictionary: %w", err)
	}
	unitStrings, err := parseDatadogStringTable(raw.dictUnitStrRaw)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse unit dictionary: %w", err)
	}
	tagSets, err := parseDatadogTagSets(raw.dictTagsetsRaw, tagStrings)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse tag sets: %w", err)
	}
	resourceSets, err := parseDatadogResourceSets(raw.dictResourceLenRaw, raw.dictResourceTypeRaw, raw.dictResourceNameRaw, resourceStrings)
	if err != nil {
		return datadogSeriesV3Decoded{}, fmt.Errorf("parse resource sets: %w", err)
	}

	return datadogSeriesV3Decoded{
		names:           names,
		tagSets:         tagSets,
		resourceSets:    resourceSets,
		sourceTypeNames: sourceTypeNames,
		originInfo:      parseDatadogOriginInfo(raw.dictOriginInfoRaw),
		unitStrings:     unitStrings,
		typesRaw:        raw.typesRaw,
		nameRefs:        decodeDatadogDeltaRefs(raw.nameRefsRaw),
		tagsetRefs:      decodeDatadogDeltaRefs(raw.tagsetRefsRaw),
		resourceRefs:    decodeDatadogDeltaRefs(raw.resourcesRefsRaw),
		intervalsRaw:    raw.intervalsRaw,
		numPointsRaw:    raw.numPointsRaw,
		sourceRefs:      decodeDatadogDeltaRefs(raw.sourceTypeNameRefs),
		originRefs:      decodeDatadogDeltaRefs(raw.originInfoRefsRaw),
		unitRefs:        decodeDatadogDeltaRefs(raw.unitRefsRaw),
		timestamps:      decodeDatadogDeltaValues(raw.timestampsRaw),
		valsSint64Raw:   raw.valsSint64Raw,
		valsFloat32Raw:  raw.valsFloat32Raw,
		valsFloat64Raw:  raw.valsFloat64Raw,
	}, nil
}

func (decoded datadogSeriesV3Decoded) buildPayload() (map[string]any, error) {
	if len(decoded.typesRaw) == 0 {
		return nil, errors.New("metric series payload had no series")
	}

	cursor := datadogSeriesV3PointCursor{}
	series := make([]any, 0, len(decoded.typesRaw))
	for i, typ := range decoded.typesRaw {
		row, err := decoded.buildSeriesRow(i, typ, &cursor)
		if err != nil {
			return nil, err
		}
		series = append(series, row)
	}
	return map[string]any{"series": series}, nil
}

func (decoded datadogSeriesV3Decoded) buildSeriesRow(index int, typ uint64, cursor *datadogSeriesV3PointCursor) (map[string]any, error) {
	metricType := int(typ & 0x0f)
	if metricType == datadogMetricTypeSketch {
		return nil, errors.New("sketch metrics are not supported in /series protobuf decoder")
	}

	row := map[string]any{}
	if metric := datadogRefString(decoded.names, datadogRefAt(decoded.nameRefs, index)); metric != "" {
		row["metric"] = metric
	}
	if tags := datadogRefStringSlice(decoded.tagSets, datadogRefAt(decoded.tagsetRefs, index)); len(tags) > 0 {
		row["tags"] = stringsToAny(tags)
	}
	if resources := datadogRefResourceSlice(decoded.resourceSets, datadogRefAt(decoded.resourceRefs, index)); len(resources) > 0 {
		row["resources"] = resources
	}
	if metricTypeName := datadogMetricTypeName(metricType); metricTypeName != "" {
		row["type"] = metricTypeName
	}
	if interval := datadogUint64At(decoded.intervalsRaw, index); interval > 0 {
		row["interval"] = float64(interval)
	}
	if sourceType := datadogRefString(decoded.sourceTypeNames, datadogRefAt(decoded.sourceRefs, index)); sourceType != "" {
		row["source_type_name"] = sourceType
	}
	if unit := datadogRefString(decoded.unitStrings, datadogRefAt(decoded.unitRefs, index)); unit != "" {
		row["unit"] = unit
	}
	if origin := datadogRefOrigin(decoded.originInfo, datadogRefAt(decoded.originRefs, index)); len(origin) > 0 {
		row["metadata"] = origin
	}

	points, err := decoded.consumeSeriesPoints(index, typ&0xf0, cursor)
	if err != nil {
		return nil, err
	}
	if len(points) > 0 {
		row["points"] = points
	}
	return row, nil
}

func (decoded datadogSeriesV3Decoded) consumeSeriesPoints(index int, valueType uint64, cursor *datadogSeriesV3PointCursor) ([]any, error) {
	pointCount := int(datadogUint64At(decoded.numPointsRaw, index))
	if pointCount < 0 || cursor.timestampIndex+pointCount > len(decoded.timestamps) {
		return nil, fmt.Errorf("series[%d] point count exceeded timestamp payload", index)
	}

	points := make([]any, 0, pointCount)
	for pointIndex := range pointCount {
		value, nextSint, nextFloat32, nextFloat64, err := datadogConsumePointValue(
			valueType,
			cursor.sintIndex,
			cursor.float32Index,
			cursor.float64Index,
			decoded.valsSint64Raw,
			decoded.valsFloat32Raw,
			decoded.valsFloat64Raw,
		)
		if err != nil {
			return nil, fmt.Errorf("series[%d] point[%d]: %w", index, pointIndex, err)
		}
		cursor.sintIndex = nextSint
		cursor.float32Index = nextFloat32
		cursor.float64Index = nextFloat64

		points = append(points, []any{
			float64(decoded.timestamps[cursor.timestampIndex]),
			value,
		})
		cursor.timestampIndex++
	}
	return points, nil
}

func (collector *datadogSeriesV2Collector) consumeField(row map[string]any, num protowire.Number, typ protowire.Type, body []byte) ([]byte, error) {
	switch num {
	case 1:
		raw, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, err
		}
		resource, err := parseDatadogSeriesProtoV2Resource(raw)
		if err != nil {
			return nil, err
		}
		collector.resources = append(collector.resources, resource)
		return next, nil
	case 2:
		value, next, err := consumeStringField(typ, body)
		if err != nil {
			return nil, err
		}
		row["metric"] = value
		return next, nil
	case 3:
		value, next, err := consumeStringField(typ, body)
		if err != nil {
			return nil, err
		}
		collector.tags = append(collector.tags, value)
		return next, nil
	case 4:
		raw, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, err
		}
		point, err := parseDatadogSeriesProtoV2Point(raw)
		if err != nil {
			return nil, err
		}
		collector.points = append(collector.points, point)
		return next, nil
	case 5:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, err
		}
		if metricType := datadogMetricTypeName(int(value)); metricType != "" {
			row["type"] = metricType
		}
		return next, nil
	case 6:
		return consumeV2StringField(row, "unit", typ, body)
	case 7:
		return consumeV2StringField(row, "source_type_name", typ, body)
	case 8:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, err
		}
		row["interval"] = float64(value)
		return next, nil
	case 9:
		raw, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, err
		}
		metadata, err := parseDatadogSeriesProtoV2Metadata(raw)
		if err != nil {
			return nil, err
		}
		collector.metadata = metadata
		return next, nil
	default:
		return skipFieldValue(num, typ, body)
	}
}

func (collector *datadogSeriesV2Collector) finalize(row map[string]any) {
	if len(collector.tags) > 0 {
		row["tags"] = collector.tags
	}
	if len(collector.points) > 0 {
		row["points"] = collector.points
	}
	if len(collector.resources) > 0 {
		row["resources"] = collector.resources
	}
	if len(collector.metadata) > 0 {
		row["metadata"] = collector.metadata
	}
}

func consumeV2StringField(row map[string]any, key string, typ protowire.Type, body []byte) ([]byte, error) {
	value, next, err := consumeStringField(typ, body)
	if err != nil {
		return nil, err
	}
	row[key] = value
	return next, nil
}

func consumeV3Bytes(body []byte, typ protowire.Type, target *[]byte) ([]byte, error) {
	raw, next, err := consumeBytesField(typ, body)
	if err != nil {
		return nil, err
	}
	*target = raw
	return next, nil
}

func consumeV3PackedSint64(body []byte, typ protowire.Type, target *[]int64) ([]byte, error) {
	values, next, err := consumePackedSint64Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func consumeV3PackedInt64(body []byte, typ protowire.Type, target *[]int64) ([]byte, error) {
	values, next, err := consumePackedInt64Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func consumeV3PackedInt32(body []byte, typ protowire.Type, target *[]int64) ([]byte, error) {
	values, next, err := consumePackedInt32Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func consumeV3PackedUint64(body []byte, typ protowire.Type, target *[]uint64) ([]byte, error) {
	values, next, err := consumePackedUint64Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func consumeV3PackedFloat32(body []byte, typ protowire.Type, target *[]float64) ([]byte, error) {
	values, next, err := consumePackedFloat32Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func consumeV3PackedFloat64(body []byte, typ protowire.Type, target *[]float64) ([]byte, error) {
	values, next, err := consumePackedFloat64Field(typ, body)
	if err != nil {
		return nil, err
	}
	*target = append(*target, values...)
	return next, nil
}

func parseDatadogSeriesProtoV2Resource(body []byte) (map[string]any, error) {
	resource := map[string]any{}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			resource["type"] = value
		case 2:
			value, next, err := consumeStringField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			resource["name"] = value
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	return resource, nil
}

func parseDatadogSeriesProtoV2Point(body []byte) ([]any, error) {
	var (
		value     float64
		timestamp int64
	)

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			fixed, next, err := consumeFixed64Field(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			value = math.Float64frombits(fixed)
		case 2:
			raw, next, err := consumeVarintField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			timestamp = int64(raw)
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	return []any{float64(timestamp), value}, nil
}

func parseDatadogSeriesProtoV2Metadata(body []byte) (map[string]any, error) {
	out := map[string]any{}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		switch num {
		case 1:
			raw, next, err := consumeBytesField(typ, body)
			if err != nil {
				return nil, err
			}
			body = next
			origin, err := parseDatadogSeriesProtoV2Origin(raw)
			if err != nil {
				return nil, err
			}
			if len(origin) > 0 {
				out["origin"] = origin
			}
		default:
			next, err := skipFieldValue(num, typ, body)
			if err != nil {
				return nil, err
			}
			body = next
		}
	}

	return out, nil
}

func parseDatadogSeriesProtoV2Origin(body []byte) (map[string]any, error) {
	out := map[string]any{}

	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]

		raw, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, err
		}
		body = next

		switch num {
		case 4:
			out["origin_product"] = float64(raw)
		case 5:
			out["origin_category"] = float64(raw)
		case 6:
			out["origin_service"] = float64(raw)
		default:
		}
	}

	return out, nil
}

func parseDatadogStringTable(body []byte) ([]string, error) {
	values := make([]string, 0)
	for len(body) > 0 {
		value, n := protowire.ConsumeString(body)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		body = body[n:]
		values = append(values, value)
	}
	return values, nil
}

func parseDatadogTagSets(raw []int64, dictionary []string) ([][]string, error) {
	sets := make([][]string, 0)
	offset := 0

	for offset < len(raw) {
		rawLength := raw[offset]
		offset++
		if rawLength < 0 {
			return nil, fmt.Errorf("negative set length %d", rawLength)
		}
		length := int(rawLength)
		if offset+length > len(raw) {
			return nil, errors.New("set length exceeded ref payload")
		}

		set := make([]string, 0, length)
		current := 0
		for _, delta := range raw[offset : offset+length] {
			current += int(delta)
			if value := datadogRefString(dictionary, current); value != "" {
				set = append(set, value)
			}
		}
		offset += length
		sets = append(sets, set)
	}

	return sets, nil
}

func parseDatadogResourceSets(lengths, typeRefs, nameRefs []int64, dictionary []string) ([]any, error) {
	out := make([]any, 0, len(lengths))
	typeOffset := 0
	nameOffset := 0

	for _, rawLength := range lengths {
		if rawLength < 0 {
			return nil, fmt.Errorf("negative resource set length %d", rawLength)
		}
		length := int(rawLength)
		if typeOffset+length > len(typeRefs) || nameOffset+length > len(nameRefs) {
			return nil, errors.New("resource set length exceeded ref payload")
		}

		currentType := 0
		currentName := 0
		resources := make([]any, 0, length)
		for i := range length {
			currentType += int(typeRefs[typeOffset+i])
			currentName += int(nameRefs[nameOffset+i])
			resource := map[string]any{}
			if value := datadogRefString(dictionary, currentType); value != "" {
				resource["type"] = value
			}
			if value := datadogRefString(dictionary, currentName); value != "" {
				resource["name"] = value
			}
			resources = append(resources, resource)
		}

		typeOffset += length
		nameOffset += length
		out = append(out, resources)
	}

	if typeOffset != len(typeRefs) || nameOffset != len(nameRefs) {
		return nil, errors.New("leftover refs after decoding resource dictionary")
	}

	return out, nil
}

func parseDatadogOriginInfo(raw []int64) []map[string]any {
	out := make([]map[string]any, 0, len(raw)/3)
	for i := 0; i+2 < len(raw); i += 3 {
		entry := map[string]any{}
		if raw[i] != 0 {
			entry["origin_product"] = float64(raw[i])
		}
		if raw[i+1] != 0 {
			entry["origin_category"] = float64(raw[i+1])
		}
		if raw[i+2] != 0 {
			entry["origin_service"] = float64(raw[i+2])
		}
		out = append(out, entry)
	}
	return out
}

func decodeDatadogDeltaRefs(raw []int64) []int {
	out := make([]int, 0, len(raw))
	current := 0
	for _, delta := range raw {
		current += int(delta)
		out = append(out, current)
	}
	return out
}

func decodeDatadogDeltaValues(raw []int64) []int64 {
	out := make([]int64, 0, len(raw))
	var current int64
	for _, delta := range raw {
		current += delta
		out = append(out, current)
	}
	return out
}

func datadogConsumePointValue(
	valueType uint64,
	sintIndex, float32Index, float64Index int,
	valsSint64 []int64,
	valsFloat32, valsFloat64 []float64,
) (float64, int, int, int, error) {
	switch valueType {
	case datadogValueTypeZero:
		return 0, sintIndex, float32Index, float64Index, nil
	case datadogValueTypeSint64:
		if sintIndex >= len(valsSint64) {
			return 0, sintIndex, float32Index, float64Index, errors.New("missing sint64 value")
		}
		return float64(valsSint64[sintIndex]), sintIndex + 1, float32Index, float64Index, nil
	case datadogValueTypeFloat32:
		if float32Index >= len(valsFloat32) {
			return 0, sintIndex, float32Index, float64Index, errors.New("missing float32 value")
		}
		return valsFloat32[float32Index], sintIndex, float32Index + 1, float64Index, nil
	case datadogValueTypeFloat64:
		if float64Index >= len(valsFloat64) {
			return 0, sintIndex, float32Index, float64Index, errors.New("missing float64 value")
		}
		return valsFloat64[float64Index], sintIndex, float32Index, float64Index + 1, nil
	default:
		return 0, sintIndex, float32Index, float64Index, fmt.Errorf("unsupported value type 0x%x", valueType)
	}
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

func datadogRefString(values []string, ref int) string {
	if ref <= 0 || ref > len(values) {
		return ""
	}
	return values[ref-1]
}

func datadogRefStringSlice(values [][]string, ref int) []string {
	if ref <= 0 || ref > len(values) {
		return nil
	}
	return values[ref-1]
}

func datadogRefResourceSlice(values []any, ref int) []any {
	if ref <= 0 || ref > len(values) {
		return nil
	}
	resourceSet, _ := values[ref-1].([]any)
	return resourceSet
}

func datadogRefOrigin(values []map[string]any, ref int) map[string]any {
	if ref <= 0 || ref > len(values) {
		return nil
	}
	return values[ref-1]
}

func datadogRefAt(values []int, index int) int {
	if index < 0 || index >= len(values) {
		return 0
	}
	return values[index]
}

func datadogUint64At(values []uint64, index int) uint64 {
	if index < 0 || index >= len(values) {
		return 0
	}
	return values[index]
}

func stringsToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func consumeBytesField(typ protowire.Type, body []byte) ([]byte, []byte, error) {
	if typ != protowire.BytesType {
		return nil, nil, fmt.Errorf("expected bytes field, got wire type %d", typ)
	}
	value, n := protowire.ConsumeBytes(body)
	if n < 0 {
		return nil, nil, protowire.ParseError(n)
	}
	return value, body[n:], nil
}

func consumeStringField(typ protowire.Type, body []byte) (string, []byte, error) {
	if typ != protowire.BytesType {
		return "", nil, fmt.Errorf("expected string field, got wire type %d", typ)
	}
	value, n := protowire.ConsumeString(body)
	if n < 0 {
		return "", nil, protowire.ParseError(n)
	}
	return value, body[n:], nil
}

func consumeVarintField(typ protowire.Type, body []byte) (uint64, []byte, error) {
	if typ != protowire.VarintType {
		return 0, nil, fmt.Errorf("expected varint field, got wire type %d", typ)
	}
	value, n := protowire.ConsumeVarint(body)
	if n < 0 {
		return 0, nil, protowire.ParseError(n)
	}
	return value, body[n:], nil
}

func consumeFixed64Field(typ protowire.Type, body []byte) (uint64, []byte, error) {
	if typ != protowire.Fixed64Type {
		return 0, nil, fmt.Errorf("expected fixed64 field, got wire type %d", typ)
	}
	value, n := protowire.ConsumeFixed64(body)
	if n < 0 {
		return 0, nil, protowire.ParseError(n)
	}
	return value, body[n:], nil
}

func consumePackedUint64Field(typ protowire.Type, body []byte) ([]uint64, []byte, error) {
	switch typ {
	case protowire.VarintType:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		return []uint64{value}, next, nil
	case protowire.BytesType:
		packed, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		values := make([]uint64, 0)
		for len(packed) > 0 {
			value, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			packed = packed[n:]
			values = append(values, value)
		}
		return values, next, nil
	default:
		return nil, nil, fmt.Errorf("expected packed uint64 field, got wire type %d", typ)
	}
}

func consumePackedSint64Field(typ protowire.Type, body []byte) ([]int64, []byte, error) {
	switch typ {
	case protowire.VarintType:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		return []int64{protowire.DecodeZigZag(value)}, next, nil
	case protowire.BytesType:
		packed, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		values := make([]int64, 0)
		for len(packed) > 0 {
			value, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			packed = packed[n:]
			values = append(values, protowire.DecodeZigZag(value))
		}
		return values, next, nil
	default:
		return nil, nil, fmt.Errorf("expected packed sint64 field, got wire type %d", typ)
	}
}

func consumePackedInt64Field(typ protowire.Type, body []byte) ([]int64, []byte, error) {
	switch typ {
	case protowire.VarintType:
		value, next, err := consumeVarintField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		return []int64{int64(value)}, next, nil
	case protowire.BytesType:
		packed, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		values := make([]int64, 0)
		for len(packed) > 0 {
			value, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			packed = packed[n:]
			values = append(values, int64(value))
		}
		return values, next, nil
	default:
		return nil, nil, fmt.Errorf("expected packed int64 field, got wire type %d", typ)
	}
}

func consumePackedInt32Field(typ protowire.Type, body []byte) ([]int64, []byte, error) {
	values, next, err := consumePackedInt64Field(typ, body)
	if err != nil {
		return nil, nil, err
	}
	return values, next, nil
}

func consumePackedFloat32Field(typ protowire.Type, body []byte) ([]float64, []byte, error) {
	switch typ {
	case protowire.Fixed32Type:
		value, n := protowire.ConsumeFixed32(body)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		return []float64{float64(math.Float32frombits(value))}, body[n:], nil
	case protowire.BytesType:
		packed, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		values := make([]float64, 0)
		for len(packed) > 0 {
			value, n := protowire.ConsumeFixed32(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			packed = packed[n:]
			values = append(values, float64(math.Float32frombits(value)))
		}
		return values, next, nil
	default:
		return nil, nil, fmt.Errorf("expected packed float32 field, got wire type %d", typ)
	}
}

func consumePackedFloat64Field(typ protowire.Type, body []byte) ([]float64, []byte, error) {
	switch typ {
	case protowire.Fixed64Type:
		value, next, err := consumeFixed64Field(typ, body)
		if err != nil {
			return nil, nil, err
		}
		return []float64{math.Float64frombits(value)}, next, nil
	case protowire.BytesType:
		packed, next, err := consumeBytesField(typ, body)
		if err != nil {
			return nil, nil, err
		}
		values := make([]float64, 0)
		for len(packed) > 0 {
			value, n := protowire.ConsumeFixed64(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			packed = packed[n:]
			values = append(values, math.Float64frombits(value))
		}
		return values, next, nil
	default:
		return nil, nil, fmt.Errorf("expected packed float64 field, got wire type %d", typ)
	}
}

func skipFieldValue(num protowire.Number, typ protowire.Type, body []byte) ([]byte, error) {
	n := protowire.ConsumeFieldValue(num, typ, body)
	if n < 0 {
		return nil, protowire.ParseError(n)
	}
	return body[n:], nil
}
