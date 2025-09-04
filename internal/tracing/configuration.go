package tracing

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const (
	// gitlabTracingEnvKey is the environment variable that contains the
	// tracing configuration in the form of a connection string.
	// The value of that variable is in one of those two format:
	// 1. opentracing://<driver>?option1=value1?option2=value2
	// 2. http://host:port?option1=value1?option2=value2
	// This format is a legacy of the LabKit library used previously.
	// The original parsing logic can be found here:
	// https://gitlab.com/gitlab-org/labkit/-/blob/v1.31.2/tracing/connstr/connection_string_parser.go?ref_type=tags#L17
	gitlabTracingEnvKey = "GITLAB_TRACING"

	// optHeaderPrefix is the prefix used to provide HTTP headers into the
	// GITLAB_TRACING connection string.
	// When using OpenTelemetry, authentication to tracing backends is done by
	// providing tokens or API keys through HTTP headers.
	// Example: opentracing://datadog?option1=value1?header^DD_API_KEY=apikey
	// In the example above, DD_API_KEY=apikey will be parsed as a header and
	// included in each request to the Datadog backend.
	optHeaderPrefix = "header^"

	// optSamplerParameter is the expected key in the connection string to provide the sampler parameter
	optSamplerParameter = "sampler_param"

	otelGrpcProtocol = "otlp-grpc"
	otelHTTPProtocol = "otlp-http"

	otlpGrpcDefaultEndpoint = "localhost:4317"
	otlpHTTPDefaultEndpoint = "localhost:4318"

	vendorStackDriver = "stackdriver"

	defaultSamplerParamValue = 0.001
)

var errInvalidConfiguration = errors.New("invalid configuration")

// tracingConfig holds the configuration extracted from GITLAB_TRACING
type tracingConfig struct {
	// protocol to use when collector endpoint is accessible locally: otlp-grpc or otlp-http
	protocol string

	// endpoint of the OTEL collector
	endpoint string

	// vendor is the vendor that provides the tracing backend.
	// This field is set when the collector is managed by a cloud provider such as stackdriver.
	// If the collector is running locally or can be accessed by a URL directly
	// (without the need of an SDK such as Google Cloud SDK), this field must remain empty
	// and `protocol` and `endpoint` must be set.
	vendor string

	// insecure relates to TLS/SSL.
	// If true, HTTP will be used.
	// If false, HTTPs will be used.
	// Default is true (HTTP).
	insecure bool

	// serviceName is he name of the service being instrumented.
	serviceName string

	// samplingParam is a float value indicating the ratio to use for sampling.
	// 1.0 = 100%
	// 0.0 = 0%
	samplingParam float64

	// headers are included in each request sent to the collector endpoint.
	// This is useful to pass along information such as API keys or tokens for authentication.
	headers map[string]string
}

// parseConnectionString parses a connection string and extracts tracing configuration.
// It returns an error if the configuration is invalid.
func parseConnectionString(connStr string) (cfg tracingConfig, err error) {
	// Handle empty connection string
	if connStr == "" {
		return tracingConfig{},
			fmt.Errorf("%w: empty connection string", errInvalidConfiguration)
	}

	// Parse URL
	u, err := url.Parse(connStr)
	if err != nil {
		return tracingConfig{},
			fmt.Errorf("%w: failed to parse connection string: %w", errInvalidConfiguration, err)
	}

	options := make(map[string]string)
	headers := make(map[string]string)

	// Extract options from query parameters
	for k, v := range u.Query() {
		if len(v) == 0 {
			continue
		}
		// if the query param is a header, add it to the header map
		// else it is an option
		if h, ok := strings.CutPrefix(k, optHeaderPrefix); ok {
			headers[h] = v[0]
		} else {
			options[k] = v[0]
		}
	}

	var endpoint string

	// Extract protocol
	protocol := u.Scheme
	if protocol == "" {
		return tracingConfig{},
			fmt.Errorf("%w: no protocol specified in connection string", errInvalidConfiguration)
	}

	vendor := ""
	switch protocol {
	case otelGrpcProtocol, otelHTTPProtocol:
		// For OpenTelemetry, the format is:
		// otlp-grpc://localhost:4317?opt1=val1&opt2=val2
		// otlp-http://localhost:4318?opt1=val1&opt2=val2
		endpoint = u.Host
		if endpoint == "" {
			return tracingConfig{},
				fmt.Errorf("%w: no endpoint specified in connection string", errInvalidConfiguration)
		}
		if _, _, err := net.SplitHostPort(endpoint); err != nil {
			return tracingConfig{},
				fmt.Errorf("%w: invalid endpoint format %q: %w", errInvalidConfiguration, endpoint, err)
		}

	case "opentracing":
		// When `opentracing` is used as the protocol, the format of the connection string is:
		// opentracing://<provider>?opt1=val1&opt2=val2
		// OpenTracing is the legacy distributed tracing protocol, and is supported here for backward
		// compatibility reasons.
		// When using `opentracing`, only Stackdriver and Jaeger are supported.
		// If the provider is not `jaeger` or `stackdriver`, it defaults to using OpenTelemetry
		// protocol on the default gRPC endpoint.
		provider := u.Host
		if provider == "" {
			return tracingConfig{},
				fmt.Errorf("%w: no provider specified in opentracing connection string", errInvalidConfiguration)
		}

		// Map OpenTracing providers to OpenTelemetry protocols
		switch provider {
		case vendorStackDriver:
			vendor = provider
			protocol = ""
			endpoint = ""
		default:
			// Jaeger is a special case because it is not a vendor
			protocol = otelGrpcProtocol
			endpoint = otlpGrpcDefaultEndpoint
			if udpEndpoint, ok := options["udp_endpoint"]; ok {
				// Parse the UDP endpoint to extract host:port
				host, _, err := net.SplitHostPort(udpEndpoint)
				if err == nil && host != "" {
					// Use the host from UDP endpoint but with the OTLP port
					endpoint = net.JoinHostPort(host, "4317")
				}

			}
		}
	default:
		return tracingConfig{},
			fmt.Errorf("%w: unsupported protocol (%s)", errInvalidConfiguration, protocol)
	}

	// Check for insecure options
	insecure := true
	if val, ok := options["insecure"]; ok {
		pval, err := strconv.ParseBool(val)
		if err != nil {
			return tracingConfig{},
				fmt.Errorf("%w: insecure option must be a boolean value: %w", errInvalidConfiguration, err)
		}
		insecure = pval
	}

	samplerFloatValue := defaultSamplerParamValue

	// Check for sampler value in options
	// If the option is not defined, we assume default value
	if !strings.EqualFold(options[optSamplerParameter], "") {
		f, err := strconv.ParseFloat(options[optSamplerParameter], 64)
		if err != nil {
			return tracingConfig{},
				fmt.Errorf("%w: %s must be a float value: %w", errInvalidConfiguration, optSamplerParameter, err)
		}

		if f > 1.0 || f < 0.0 {
			return tracingConfig{},
				fmt.Errorf("%s is '%v' but must be a float value between 0.0 and 1.0", optSamplerParameter, samplerFloatValue)
		}

		samplerFloatValue = clampValue(f, 0.0, 1.0)
	}

	return tracingConfig{
		protocol:      protocol,
		endpoint:      endpoint,
		vendor:        vendor,
		insecure:      insecure,
		samplingParam: samplerFloatValue,
		headers:       headers,
	}, nil
}

func clampValue(val, min, max float64) float64 {
	if val > max {
		return 1.0
	} else if val < min {
		return 0.0
	}
	return val
}
