package sdk

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// DefaultExcludedOperations are dropped by the sampler regardless of the
// ratio: long-lived Pub/Sub streaming pulls and gRPC health/reflection calls.
var DefaultExcludedOperations = []string{
	"google.pubsub.v1.Subscriber/StreamingPull",
	"grpc.health.v1.Health/",
	"grpc.reflection.",
}

// NewSampler returns the default sampler: spans whose name (or rpc.method /
// rpc.service attribute) contains one of DefaultExcludedOperations or exclude
// are dropped; everything else follows ParentBased(TraceIDRatioBased(ratio)).
func NewSampler(ratio float64, exclude []string) sdktrace.Sampler {
	return newFilterSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), exclude)
}

func newFilterSampler(base sdktrace.Sampler, exclude []string) sdktrace.Sampler {
	seen := map[string]bool{}
	var patterns []string
	for _, p := range append(append([]string(nil), DefaultExcludedOperations...), exclude...) {
		if p != "" && !seen[p] {
			seen[p] = true
			patterns = append(patterns, p)
		}
	}
	return &filterSampler{base: base, patterns: patterns}
}

type filterSampler struct {
	base     sdktrace.Sampler
	patterns []string
}

func (s *filterSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if s.excluded(p) {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.Drop,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	return s.base.ShouldSample(p)
}

func (s *filterSampler) excluded(p sdktrace.SamplingParameters) bool {
	if s.matches(p.Name) {
		return true
	}
	for _, a := range p.Attributes {
		if (a.Key == "rpc.method" || a.Key == "rpc.service") && s.matches(a.Value.AsString()) {
			return true
		}
	}
	return false
}

func (s *filterSampler) matches(v string) bool {
	if v == "" {
		return false
	}
	for _, p := range s.patterns {
		if strings.Contains(v, p) {
			return true
		}
	}
	return false
}

func (s *filterSampler) Description() string {
	return "OperationFilter{" + s.base.Description() + "}"
}

// sampler resolves the sampler. OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG
// override Config.SampleRatio. The operation filter always applies.
func sampler(cfg Config) (sdktrace.Sampler, error) {
	ratio := 1.0
	if cfg.SampleRatio != nil {
		ratio = *cfg.SampleRatio
		if ratio < 0 || ratio > 1 {
			return nil, fmt.Errorf("SampleRatio %v out of range [0,1]", ratio)
		}
	}
	name := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")))
	arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
	if arg != "" && (name == "" || strings.HasSuffix(name, "traceidratio")) {
		r, err := strconv.ParseFloat(arg, 64)
		if err != nil || r < 0 || r > 1 {
			return nil, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG %q: want a number in [0,1]", arg)
		}
		ratio = r
	}
	parentBased := func(root sdktrace.Sampler) sdktrace.Sampler {
		if cfg.IgnoreRemoteSampled {
			return sdktrace.ParentBased(root,
				sdktrace.WithRemoteParentSampled(root), sdktrace.WithRemoteParentNotSampled(root))
		}
		return sdktrace.ParentBased(root)
	}
	var base sdktrace.Sampler
	switch name {
	case "", "parentbased_traceidratio":
		base = parentBased(sdktrace.TraceIDRatioBased(ratio))
	case "traceidratio":
		base = sdktrace.TraceIDRatioBased(ratio)
	case "always_on":
		base = sdktrace.AlwaysSample()
	case "always_off":
		base = sdktrace.NeverSample()
	case "parentbased_always_on":
		base = parentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		base = parentBased(sdktrace.NeverSample())
	default:
		return nil, fmt.Errorf("OTEL_TRACES_SAMPLER %q is not supported", name)
	}
	return newFilterSampler(base, cfg.ExcludeOperations), nil
}
