package gitalyauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	grpcmwauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	//nolint:gochecknoglobals
	// This infrastructure is required for testing purposes and there is no
	// proper place to put it instead. While we could move it into the
	// config, we certainly don't want to make it configurable for now, so
	// it'd be a bad fit there.
	tokenValidityDuration = 30 * time.Second

	errUnauthenticated = status.Errorf(codes.Unauthenticated, "authentication required")

	authErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_authentication_errors_total",
			Help: "Counts of Gitaly request authentication errors",
		},
		[]string{"version", "error"},
	)
)

const tokenVersionV2 = "v2"

func newPermissionDeniedError(reason string) error {
	return status.Errorf(codes.PermissionDenied, "permission denied: %s", reason)
}

// TokenValidityDuration returns the duration for which any token will be
// valid. This is currently only used by our testing infrastructure.
func TokenValidityDuration() time.Duration {
	return tokenValidityDuration
}

// SetTokenValidityDuration changes the duration for which any token will be
// valid. It only applies to newly created tokens.
func SetTokenValidityDuration(d time.Duration) {
	tokenValidityDuration = d
}

// AuthInfo contains the authentication information coming from a request
type AuthInfo struct {
	Version       string
	SignedMessage []byte
	Message       string
}

// CheckToken checks the 'authentication' header of incoming gRPC
// metadata in ctx. It returns nil if and only if the token matches
// secret.
func CheckToken(ctx context.Context, secret string, targetTime time.Time) error {
	if len(secret) == 0 {
		return status.Errorf(codes.Unauthenticated, "secret must not be empty")
	}

	authInfo, err := ExtractAuthInfo(ctx)
	if err != nil {
		return errUnauthenticated
	}

	if authInfo.Version != tokenVersionV2 {
		return newPermissionDeniedError("invalid token version")
	}

	return v2HmacInfoValid(authInfo.Message, authInfo.SignedMessage, []byte(secret), targetTime, tokenValidityDuration)
}

// ExtractAuthInfo returns an `AuthInfo` with the data extracted from `ctx`
func ExtractAuthInfo(ctx context.Context) (*AuthInfo, error) {
	token, err := grpcmwauth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	split := strings.SplitN(token, ".", 3)

	if len(split) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}

	version, sig, msg := split[0], split[1], split[2]
	decodedSig, err := hex.DecodeString(sig)
	if err != nil {
		return nil, err
	}

	return &AuthInfo{Version: version, SignedMessage: decodedSig, Message: msg}, nil
}

func countV2Error(message string) { authErrors.WithLabelValues(tokenVersionV2, message).Inc() }

func v2HmacInfoValid(message string, signedMessage, secret []byte, targetTime time.Time, tokenValidity time.Duration) error {
	expectedHMAC := hmacSign(secret, message)
	if !hmac.Equal(signedMessage, expectedHMAC) {
		const reason = "wrong hmac signature"
		countV2Error(reason)
		return newPermissionDeniedError(reason)
	}

	timestamp, err := strconv.ParseInt(message, 10, 64)
	if err != nil {
		const reason = "cannot parse timestamp"
		countV2Error(reason)
		return newPermissionDeniedError(fmt.Sprintf("%s: %s", reason, err))
	}

	issuedAt := time.Unix(timestamp, 0)
	lowerBound := targetTime.Add(-tokenValidity)
	upperBound := targetTime.Add(tokenValidity)

	if issuedAt.Before(lowerBound) {
		const reason = "token has expired"
		countV2Error(reason)
		return newPermissionDeniedError(reason)
	}

	if issuedAt.After(upperBound) {
		const reason = "token's validity window is in future"
		countV2Error(reason)
		return newPermissionDeniedError(reason)
	}

	return nil
}

func hmacSign(secret []byte, message string) []byte {
	mac := hmac.New(sha256.New, secret)
	// hash.Hash never returns an error.
	_, _ = mac.Write([]byte(message))

	return mac.Sum(nil)
}
