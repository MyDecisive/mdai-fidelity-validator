workspace "mdai-fidelity-validator" "Architecture of the telemetry fidelity comparison service" {
    model {
        operator = person "Telemetry operator" "Defines fidelity policy, monitors Prometheus, and investigates comparison results."

        telemetryPipeline = softwareSystem "Telemetry pipeline" "Receiver, processors, and exporter whose attribute fidelity is being validated." {
            receiver = container "Receiver side" "Produces or mirrors the ingress Datadog wire payload." "Telemetry receiver"
            exporter = container "Exporter side" "Produces or mirrors the egress Datadog wire payload." "Telemetry exporter"
        }

        prometheus = softwareSystem "Prometheus" "Scrapes cumulative fidelity metrics and evaluates dashboards or alerts."

        validator = softwareSystem "MDAI Fidelity Validator" "Correlates receiver/exporter observations and evaluates canonical and policy fidelity." {
            capture = container "Capture listeners" "Accepts receiver, exporter, and synthetic observations; returns Datadog-compatible acknowledgements." "Go net/http :8126, :18081, :8080"
            engine = container "Fidelity engine" "Decodes, maps, correlates, retains, and compares observations." "Go process memory" {
                decoder = component "Datadog decoder" "Decompresses and decodes JSON, MessagePack, series protobuf, and trace protobuf; splits natural item groups." "service_payload.go and Datadog decoders"
                mapper = component "Canonical mapper" "Flattens decoded data and maps selected paths to canonical snake_case attributes." "field_mapping.go"
                correlator = component "Correlator and pending store" "Resolves signal-prefixed identity and pairs opposite-side observations within retention." "service_correlation.go and service_observe.go"
                fullComparator = component "Full comparator" "Diffs canonical maps and trace spans." "service_compare.go and service_spans.go"
                policyEvaluator = component "Policy evaluator" "Checks declared required attributes using value or presence-only semantics." "policy.go"
                metricRecorder = component "Metric recorder" "Updates request, pending, full-comparison, and required-policy Prometheus collectors." "service_observe.go"
            }
            admin = container "Admin API" "Exposes health, pending observations, recent captures, results, and pair configuration." "Go net/http :8080"
            metrics = container "Metrics endpoint" "Exposes the process-global Prometheus registry." "promhttp :8888"
            config = container "Mapping and policy files" "Versioned YAML contract, mounted by Helm and hot-reloaded with last-known-good behavior." "YAML / ConfigMap"
        }

        receiver -> capture "Sends receiver-side observation" "HTTP"
        exporter -> capture "Sends exporter-side observation" "HTTP"
        capture -> decoder "Passes classified request"
        decoder -> mapper "Provides flattened item groups"
        mapper -> correlator "Provides canonical attributes"
        correlator -> fullComparator "Emits completed pair"
        correlator -> policyEvaluator "Emits completed pair"
        fullComparator -> metricRecorder "Provides full result"
        policyEvaluator -> metricRecorder "Provides required result"
        fullComparator -> admin "Stores comparison result"
        policyEvaluator -> admin "Stores comparison result"
        metricRecorder -> metrics "Updates collectors"
        config -> mapper "Supplies field mapping"
        config -> policyEvaluator "Supplies fidelity policy"
        prometheus -> metrics "Scrapes /metrics" "HTTP"
        operator -> prometheus "Reads dashboards and alerts"
        operator -> admin "Investigates correlations and deltas" "HTTP/JSON"
    }

    views {
        systemContext validator "SystemContext" {
            include *
            autoLayout lr
        }

        container validator "Containers" {
            include *
            autoLayout lr
        }

        component engine "FidelityEngine" {
            include *
            autoLayout lr
        }

        dynamic validator "ComparisonSequence" "Lifecycle of a completed comparison" {
            receiver -> capture "1. Send receiver observation"
            capture -> decoder "2. Decode and split request"
            decoder -> mapper "3. Build canonical attributes"
            mapper -> correlator "4. Store pending item"
            exporter -> capture "5. Send exporter observation"
            capture -> decoder "6. Decode and split request"
            decoder -> mapper "7. Build canonical attributes"
            mapper -> correlator "8. Match opposite-side item"
            correlator -> fullComparator "9. Evaluate canonical and span fidelity"
            correlator -> policyEvaluator "10. Evaluate declared contract"
            fullComparator -> metricRecorder "11. Record full result"
            policyEvaluator -> metricRecorder "12. Record required result"
            autoLayout lr
        }

        styles {
            element "Person" {
                shape person
                background #08427b
                color #ffffff
            }
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Component" {
                background #85bbf0
                color #000000
            }
        }
    }
}
