package validator

import (
	"errors"
	"fmt"
	"strings"

	dptrace "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
)

func decodeDatadogTraceMsgpack(requestPath string, body []byte) (any, error) {
	var traces dptrace.Traces
	if strings.Contains(requestPath, "/v0.5/") {
		if err := traces.UnmarshalMsgDictionary(body); err != nil {
			return nil, fmt.Errorf("unmarshal v0.5 trace payload: %w", err)
		}
	} else {
		if _, err := traces.UnmarshalMsg(body); err != nil {
			return nil, fmt.Errorf("unmarshal msgpack trace payload: %w", err)
		}
	}

	var spans []map[string]any
	for _, trace := range traces {
		for _, s := range trace {
			spans = append(spans, spanToMap(s))
		}
	}
	if len(spans) == 0 {
		return nil, errors.New("trace payload had no spans")
	}

	return groupSpansByTraceID(spans), nil
}
