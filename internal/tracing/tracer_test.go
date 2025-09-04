package tracing

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestInitializeTracerProvider tests the initialization of a tracer
// Because the configuration is already tested in configuration_test.go
// this test does not attempt to test all possible combination of configuration.
func TestInitializeTracerProvider(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		wantErr error
	}{
		{
			name: "tracing disabled",
			setup: func() {
				_ = os.Setenv(gitlabTracingEnvKey, "")
			},
			wantErr: errTracingDisabled,
		},
		{
			name: "otel-grpc tracer provider",
			setup: func() {
				_ = os.Setenv(gitlabTracingEnvKey, "otlp-grpc://localhost:1234")
			},
			wantErr: nil,
		},
		{
			name: "otel-http tracer provider",
			setup: func() {
				_ = os.Setenv(gitlabTracingEnvKey, "otlp-http://localhost:1234")
			},
			wantErr: nil,
		},
		{
			name: "opentracing tracer provider",
			setup: func() {
				_ = os.Setenv(gitlabTracingEnvKey, "opentracing://jaeger:localhost:1234")
			},
			wantErr: nil,
		},
		{
			name: "invalid tracer provider",
			setup: func() {
				_ = os.Setenv(gitlabTracingEnvKey, "opentrac://jaeger:localhost:1234")
			},
			wantErr: errInvalidConfiguration,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			ctx := context.Background()

			tp, closer, err := InitializeTracerProvider(ctx, "gitaly")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.IsType(t, tp, noop.TracerProvider{})
				return
			}

			assert.NotNil(t, tp)
			// For some cases, such as when resource creation fails, an error will be returned
			// but this error should not prevent a tracer provider to be created.
			// As such, we validate here that the tracer returned is indeed a working tracer provider
			// and not a NoopTracer.
			assert.IsNotType(t, tp, noop.TracerProvider{})
			assert.NotNil(t, closer)
			assert.Nil(t, closer.Close())
		})
	}
}
