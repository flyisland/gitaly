package client

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const secret = "it's a secret"

func TestJWTAuthenticationHeader(t *testing.T) {
	expectedCorrelationID := "testing-correlation-id"
	server := httptest.NewServer(correlation.InjectCorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprint(w, r.Header.Get(apiSecretHeaderName))
		require.NoError(t, err)

		correlationID := correlation.ExtractFromContext(r.Context())
		require.Equal(t, expectedCorrelationID, correlationID)
	}), correlation.WithPropagation()))
	defer server.Close()

	tests := []struct {
		secret string
		method string
	}{
		{
			secret: secret,
			method: http.MethodGet,
		},
		{
			secret: secret,
			method: http.MethodPost,
		},
		{
			secret: "\n\t " + secret + "\t \n",
			method: http.MethodGet,
		},
		{
			secret: "\n \t" + secret + "\n\t ",
			method: http.MethodPost,
		},
	}

	for _, tc := range tests {
		t.Run(tc.method+" with "+tc.secret, func(t *testing.T) {
			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			gitlabnet, err := NewGitlabNetClient(
				logger,
				"user",
				"password",
				tc.secret,
				&HTTPClient{Client: server.Client(), Host: server.URL},
			)
			require.NoError(t, err)

			ctx := correlation.ContextWithCorrelation(testhelper.Context(t), expectedCorrelationID)
			response, err := gitlabnet.DoRequest(ctx, tc.method, "/jwt_auth", nil)
			require.NoError(t, err)
			require.NotNil(t, response)
			defer response.Body.Close()

			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			claims := &jwt.RegisteredClaims{}
			token, err := jwt.ParseWithClaims(string(responseBody), claims, func(token *jwt.Token) (interface{}, error) {
				return []byte(secret), nil
			})
			require.NoError(t, err)
			require.True(t, token.Valid)
			require.Equal(t, "gitlab-shell", claims.Issuer)
			require.WithinDuration(t, time.Now().Truncate(time.Second), claims.IssuedAt.Time, time.Second)
			require.WithinDuration(t, time.Now().Truncate(time.Second).Add(time.Minute), claims.ExpiresAt.Time, time.Second)

			logEntry := hook.LastEntry()
			require.Equal(t, "Finished HTTP request", logEntry.Message)

			require.NotEmpty(t, logEntry.Data["correlation_id"])
		})
	}
}

func TestServerUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.Close() // We are closing the server to replicate server being unreachable

	logger := testhelper.NewLogger(t)
	hook := testhelper.AddLoggerHook(logger)

	gitlabnet, err := NewGitlabNetClient(
		logger,
		"user",
		"password",
		"secret",
		&HTTPClient{Client: server.Client(), Host: server.URL},
	)
	require.NoError(t, err)

	ctx := testhelper.Context(t)
	ctx = correlation.ContextWithCorrelation(ctx, "test-correlation-id")
	response, err := gitlabnet.DoRequest(ctx, http.MethodGet, "/test", nil)

	require.ErrorContains(t, err, "Internal API unreachable")
	require.Nil(t, response)

	logEntry := hook.LastEntry()
	require.Equal(t, "Internal API unreachable", logEntry.Message)
	require.Equal(t, "test-correlation-id", logEntry.Data["correlation_id"])
}

func TestServerErrors(t *testing.T) {
	tests := []struct {
		desc    string
		handler func(w http.ResponseWriter, r *http.Request)
		logMsg  string
		err     error
	}{
		{
			desc: "server returns error with message",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write([]byte(`{"message": "you're not allowed here'"}`))
				require.NoError(t, err)
			},
			logMsg: "Internal API error",
			err:    &APIError{Msg: "you're not allowed here'"},
		},
		{
			desc: "server returns error without message",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
			},
			logMsg: "Internal API error",
			err:    &APIError{Msg: "Internal API error (401)"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.handler))
			defer server.Close()

			logger := testhelper.NewLogger(t)
			hook := testhelper.AddLoggerHook(logger)

			gitlabnet, err := NewGitlabNetClient(
				logger,
				"user",
				"password",
				"secret",
				&HTTPClient{Client: server.Client(), Host: server.URL},
			)
			require.NoError(t, err)

			ctx := testhelper.Context(t)
			ctx = correlation.ContextWithCorrelation(ctx, "test-correlation-id")
			response, err := gitlabnet.DoRequest(ctx, http.MethodGet, "/test", nil)

			require.Error(t, err)
			require.Nil(t, response)

			logEntry := hook.LastEntry()
			require.Equal(t, tc.logMsg, logEntry.Message)
			require.Equal(t, tc.err, logEntry.Data["error"])
			require.Equal(t, "test-correlation-id", logEntry.Data["correlation_id"])
		})
	}
}

// wraps an http.RoundTripper to intercept responses
type spyTransport struct {
	base       http.RoundTripper
	onResponse func(*http.Response)
}

// wraps an io.ReadCloser to track when Close() is called
type trackableBody struct {
	io.ReadCloser
	onClose func()
}

func (s *spyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	if s.onResponse != nil {
		s.onResponse(resp)
	}

	return resp, nil
}

func (t *trackableBody) Close() error {
	if t.onClose != nil {
		t.onClose()
	}
	return t.ReadCloser.Close()
}

func TestRateLimitWithHTTPServer(t *testing.T) {
	// This test verifies the prevention of rate limit goroutine leaks.
	// Without that, each 429 response would leak a goroutine because the response body
	// is never closed when we return early with RailsRateLimitedError.
	var bodyCloseCalled bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, err := w.Write([]byte(`{"message": "rate limited"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	// Use custom transport to inject trackable body
	transport := &spyTransport{
		base: http.DefaultTransport,
		onResponse: func(resp *http.Response) {
			if resp.StatusCode == http.StatusTooManyRequests {
				originalBody := resp.Body
				resp.Body = &trackableBody{
					ReadCloser: originalBody,
					onClose: func() {
						bodyCloseCalled = true
						originalBody.Close()
					},
				}
			}
		},
	}

	client := &http.Client{Transport: transport}
	gitlabnet, err := NewGitlabNetClient(
		testhelper.NewLogger(t),
		"user",
		"password",
		"secret",
		&HTTPClient{Client: client, Host: server.URL},
	)
	require.NoError(t, err)

	ctx := testhelper.Context(t)
	_, err = gitlabnet.DoRequest(ctx, http.MethodGet, "/test", nil)

	require.Error(t, err)
	require.IsType(t, RailsRateLimitedError{}, err)
	require.True(t, bodyCloseCalled, "Response body should be closed")
}
