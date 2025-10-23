package tracing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		connStr string
		wantCfg tracingConfig
		wantErr error
	}{
		{
			name:    "otlp-grpc: when host is not specified it should return an error",
			connStr: "otlp-grpc://",
			wantCfg: tracingConfig{},
			wantErr: errInvalidConfiguration,
		},
		{
			name:    "otlp-http: when host is not specified it should return an error",
			connStr: "otlp-http://",
			wantCfg: tracingConfig{},
			wantErr: errInvalidConfiguration,
		},
		{
			name:    "otlp-grpc protocol: with headers",
			connStr: "otlp-grpc://localhost:1234?header^API-KEY=my-key",
			wantCfg: tracingConfig{
				protocol:      otelGrpcProtocol,
				endpoint:      "localhost:1234",
				insecure:      true,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-http protocol: with headers",
			connStr: "otlp-http://localhost:1234?header^API-KEY=my-key",
			wantCfg: tracingConfig{
				protocol:      otelHTTPProtocol,
				endpoint:      "localhost:1234",
				insecure:      true,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-grpc protocol: with insecure false",
			connStr: "otlp-grpc://localhost:1234?header^API-KEY=my-key&insecure=false",
			wantCfg: tracingConfig{
				protocol:      otelGrpcProtocol,
				endpoint:      "localhost:1234",
				insecure:      false,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-http protocol: with insecure false",
			connStr: "otlp-http://localhost:1234?header^API-KEY=my-key&insecure=true",
			wantCfg: tracingConfig{
				protocol:      otelHTTPProtocol,
				endpoint:      "localhost:1234",
				insecure:      true,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-grpc protocol: with insecure true",
			connStr: "otlp-grpc://localhost:1234?header^API-KEY=my-key&insecure=true",
			wantCfg: tracingConfig{
				protocol:      otelGrpcProtocol,
				endpoint:      "localhost:1234",
				insecure:      true,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-http protocol: with insecure true",
			connStr: "otlp-http://localhost:1234?header^API-KEY=my-key&insecure=true",
			wantCfg: tracingConfig{
				protocol:      otelHTTPProtocol,
				endpoint:      "localhost:1234",
				insecure:      true,
				samplingParam: defaultSamplerParamValue,
				headers:       map[string]string{"API-KEY": "my-key"},
			},
		},
		{
			name:    "otlp-http protocol: with typo in insecure value",
			connStr: "otlp-http://localhost:1234?header^API-KEY=my-key&insecure=flase",
			wantCfg: tracingConfig{},
		},
		{
			name:    "invalid provider: it should return an error",
			connStr: "something://",
			wantCfg: tracingConfig{},
			wantErr: errInvalidConfiguration,
		},
		{
			name:    "jaeger provider",
			connStr: "opentracing://jaeger?http_endpoint=127.0.0.1:1234&sampler_param=0.76",
			wantCfg: tracingConfig{
				protocol:      otelGrpcProtocol,
				endpoint:      otlpGrpcDefaultEndpoint,
				insecure:      true,
				samplingParam: 0.76,
				headers:       map[string]string{},
			},
		},
		{
			name:    "stackdriver provider",
			connStr: "opentracing://stackdriver?http_endpoint=127.0.0.1:1234&sampler_param=1.0",
			wantCfg: tracingConfig{
				insecure:      true,
				vendor:        vendorStackDriver,
				samplingParam: 1.0,
				headers:       map[string]string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCfg, err := parseConnectionString(tt.connStr)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.Equalf(t, tt.wantCfg, gotCfg, "parseConnectionString(%v)", tt.connStr)
		})
	}
}
