package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	internalAPIPath     = "/api/v4/internal"
	apiSecretHeaderName = "Gitlab-Shell-Api-Request"
	defaultUserAgent    = "GitLab-Shell"
	jwtTTL              = time.Minute
	jwtIssuer           = "gitlab-shell"
)

//nolint:revive // This is unintentionally missing documentation.
type ErrorResponse struct {
	Message string `json:"message"`
}

//nolint:revive // This is unintentionally missing documentation.
type GitlabNetClient struct {
	logger     log.Logger
	httpClient *HTTPClient
	user       string
	password   string
	secret     string
	userAgent  string
}

//nolint:revive // This is unintentionally missing documentation.
type APIError struct {
	Msg string
}

// OriginalRemoteIPContextKey is to be used as the key in a Context
// to set an X-Forwarded-For header in a request
type OriginalRemoteIPContextKey struct{}

func (e *APIError) Error() string {
	return e.Msg
}

//nolint:revive // This is unintentionally missing documentation.
func NewGitlabNetClient(
	logger log.Logger,
	user,
	password,
	secret string,
	httpClient *HTTPClient,
) (*GitlabNetClient, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("unsupported protocol")
	}

	return &GitlabNetClient{
		logger:     logger,
		httpClient: httpClient,
		user:       user,
		password:   password,
		secret:     secret,
		userAgent:  defaultUserAgent,
	}, nil
}

// SetUserAgent overrides the default user agent for the User-Agent header field
// for subsequent requests for the GitlabNetClient
func (c *GitlabNetClient) SetUserAgent(ua string) {
	c.userAgent = ua
}

func normalizePath(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if !strings.HasPrefix(path, internalAPIPath) {
		path = internalAPIPath + path
	}
	return path
}

func newRequest(ctx context.Context, method, host, path string, data interface{}) (*http.Request, error) {
	var jsonReader io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}

		jsonReader = bytes.NewReader(jsonData)
	}

	request, err := http.NewRequestWithContext(ctx, method, host+path, jsonReader)
	if err != nil {
		return nil, err
	}

	return request, nil
}

func parseError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode <= 399 {
		return nil
	}
	defer resp.Body.Close()
	parsedResponse := &ErrorResponse{}

	if err := json.NewDecoder(resp.Body).Decode(parsedResponse); err != nil {
		return &APIError{fmt.Sprintf("Internal API error (%v)", resp.StatusCode)}
	}

	return &APIError{parsedResponse.Message}
}

//nolint:revive // This is unintentionally missing documentation.
func (c *GitlabNetClient) Get(ctx context.Context, path string) (*http.Response, error) {
	return c.DoRequest(ctx, http.MethodGet, normalizePath(path), nil)
}

//nolint:revive // This is unintentionally missing documentation.
func (c *GitlabNetClient) Post(ctx context.Context, path string, data interface{}) (*http.Response, error) {
	return c.DoRequest(ctx, http.MethodPost, normalizePath(path), data)
}

//nolint:revive // This is unintentionally missing documentation.
func (c *GitlabNetClient) DoRequest(ctx context.Context, method, path string, data interface{}) (*http.Response, error) {
	request, err := newRequest(ctx, method, c.httpClient.Host, path, data)
	if err != nil {
		return nil, err
	}

	user, password := c.user, c.password
	if user != "" && password != "" {
		request.SetBasicAuth(user, password)
	}

	claims := jwt.RegisteredClaims{
		Issuer:    jwtIssuer,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(jwtTTL)),
	}
	secretBytes := []byte(strings.TrimSpace(c.secret))
	tokenString, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secretBytes)
	if err != nil {
		return nil, err
	}
	request.Header.Set(apiSecretHeaderName, tokenString)

	originalRemoteIP, ok := ctx.Value(OriginalRemoteIPContextKey{}).(string)
	if ok {
		request.Header.Add("X-Forwarded-For", originalRemoteIP)
	}

	correlationID := correlation.ExtractFromContextOrGenerate(ctx)

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("User-Agent", c.userAgent)
	request.Header.Add("X-Request-ID", correlationID)
	request.Close = true

	start := time.Now()
	response, err := c.httpClient.Do(request)

	logger := c.logger.WithFields(log.Fields{
		"method":              method,
		"url":                 request.URL.String(),
		"duration_ms":         time.Since(start) / time.Millisecond,
		correlation.FieldName: correlationID,
	})

	if err != nil {
		logger.WithError(err).Error("Internal API unreachable")
		return nil, &APIError{"Internal API unreachable"}
	}

	if response != nil {
		logger = logger.WithField("status", response.StatusCode)
	}
	if err := parseError(response); err != nil {
		logger.WithError(err).Error("Internal API error")
		return nil, err
	}

	if response.ContentLength >= 0 {
		logger = logger.WithField("content_length_bytes", response.ContentLength)
	}

	logger.Info("Finished HTTP request")

	return response, nil
}
