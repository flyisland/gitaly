package gitcmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/metadata"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// EnvHooksPayload is the name of the environment variable used
	// to hold the hooks payload.
	EnvHooksPayload = "GITALY_HOOKS_PAYLOAD"
)

// ErrPayloadNotFound is returned by HooksPayloadFromEnv if the given
// environment variables don't have a hooks payload.
var ErrPayloadNotFound = errors.New("no hooks payload found in environment")

// Hook represents a git hook. See githooks(5) for more information about
// existing hooks.
type Hook uint

const (
	// ReferenceTransactionHook represents the reference-transaction git hook.
	ReferenceTransactionHook = Hook(1 << iota)
	// UpdateHook represents the update git hook.
	UpdateHook
	// PreReceiveHook represents the pre-receive git hook.
	PreReceiveHook
	// PostReceiveHook represents the post-receive git hook.
	PostReceiveHook
	// PackObjectsHook represents the pack-objects git hook.
	PackObjectsHook
	// ProcReceiveHook represents the proc-receive git hook.
	ProcReceiveHook

	// AllHooks is the bitwise set of all hooks supported by Gitaly.
	AllHooks = ReferenceTransactionHook | UpdateHook | PreReceiveHook | PostReceiveHook | PackObjectsHook
	// ReceivePackHooks includes the set of hooks which shall be executed in
	// a typical "push" or an emulation thereof (e.g. `updateReferenceWithHooks()`).
	ReceivePackHooks = ReferenceTransactionHook | UpdateHook | PreReceiveHook | PostReceiveHook
)

// FeatureFlagWithValue is used as part of the HooksPayload to pass on feature flags with their
// values to gitaly-hooks.
type FeatureFlagWithValue struct {
	// Flag is the feature flag.
	Flag featureflag.FeatureFlag `json:"flag"`
	// Enabled indicates whether the flag is enabled or not.
	Enabled bool `json:"enabled"`
}

// HooksPayload holds parameters required for all hooks.
type HooksPayload struct {
	// RequestedHooks is a bitfield of requested Hooks. Hooks which
	// were not requested will not get executed.
	RequestedHooks Hook `json:"requested_hooks"`
	// FeatureFlagsWithValue contains feature flags with their values. They are set into the
	// outgoing context when calling HookService.
	FeatureFlagsWithValue []FeatureFlagWithValue `json:"feature_flags_with_value,omitempty"`

	// Repo is the repository in which the hook is running.
	Repo *gitalypb.Repository `json:"-"`
	// ObjectFormat is the object format used by the repository. Some hooks use it in order to
	// verify object IDs part of their input.
	ObjectFormat string `json:"object_format"`

	// RuntimeDir is the path to Gitaly's runtime directory.
	RuntimeDir string `json:"runtime_dir"`
	// InternalSocket is the path to Gitaly's internal socket.
	InternalSocket string `json:"internal_socket"`
	// InternalSocketToken is the token required to authenticate with
	// Gitaly's internal socket.
	InternalSocketToken string `json:"internal_socket_token"`

	// Transaction is used to identify a reference transaction. This is an optional field -- if
	// it's not set, no transactional voting will happen.
	Transaction *txinfo.Transaction `json:"transaction"`

	// UserDetails contains information required when executing
	// git-receive-pack or git-upload-pack
	UserDetails *UserDetails `json:"user_details"`

	// TransactionID identifies the storage transaction this hooks call runs in. It's
	// used to access the transaction in the hook manager.
	TransactionID storage.TransactionID `json:"transaction_id,omitempty"`

	// GitalyClientContext contains the context passed through the RPCs.
	GitalyClientContext []byte `json:"gitaly_client_context,omitempty"`

	// LogFields contains the log fields in the current context. They are passed
	// to the HookService so the calls can be correlated with the main RPC call.
	LogFields log.Fields
}

// UserDetails contains all information which is required for hooks
// executed by git-receive-pack, namely the pre-receive, update or post-receive
// hook.
type UserDetails struct {
	// Username contains the name of the user who has caused the hook to be executed.
	Username string `json:"username"`
	// UserID contains the ID of the user who has caused the hook to be executed.
	UserID string `json:"userid"`
	// Protocol contains the protocol via which the hook was executed. This
	// can be one of "web", "ssh" or "smarthttp".
	Protocol string `json:"protocol"`
	// RemoteIP contains the original IP of the client who initiated the flow leading to this
	// target hook.
	RemoteIP string `json:"remote_ip"`
}

// jsonHooksPayload wraps the HooksPayload such that we can manually encode the
// repository protobuf message.
type jsonHooksPayload struct {
	HooksPayload
	Repo string `json:"repository"`
}

// NewHooksPayload creates a new hooks payload which can then be encoded and
// passed to Git hooks.
func NewHooksPayload(
	ctx context.Context,
	cfg config.Cfg,
	repo storage.Repository,
	objectHash git.ObjectHash,
	tx *txinfo.Transaction,
	userDetails *UserDetails,
	requestedHooks Hook,
	featureFlagsWithValue map[featureflag.FeatureFlag]bool,
	transactionID storage.TransactionID,
) HooksPayload {
	flags := make([]FeatureFlagWithValue, 0, len(featureFlagsWithValue))
	for flag, enabled := range featureFlagsWithValue {
		flags = append(flags, FeatureFlagWithValue{
			Flag:    flag,
			Enabled: enabled,
		})
	}

	// Extract context blob from gRPC metadata
	var clientContext []byte
	if ctxValue := metadata.GetValue(ctx, metadata.ClientContextMetadataKey); ctxValue != "" {
		clientContext = []byte(ctxValue)
	}

	return HooksPayload{
		Repo: &gitalypb.Repository{
			StorageName:                   repo.GetStorageName(),
			RelativePath:                  repo.GetRelativePath(),
			GitObjectDirectory:            repo.GetGitObjectDirectory(),
			GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
			GlRepository:                  repo.GetGlRepository(),
			GlProjectPath:                 repo.GetGlProjectPath(),
		},
		ObjectFormat:          objectHash.Format,
		RuntimeDir:            cfg.RuntimeDir,
		InternalSocket:        cfg.InternalSocketPath(),
		InternalSocketToken:   cfg.Auth.GetToken(),
		Transaction:           tx,
		UserDetails:           userDetails,
		RequestedHooks:        requestedHooks,
		FeatureFlagsWithValue: flags,
		TransactionID:         transactionID,
		GitalyClientContext:   clientContext,
		LogFields:             log.FromContext(ctx).Data(),
	}
}

func lookupEnv(envs []string, key string) (string, bool) {
	for _, env := range envs {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}

		if kv[0] == key {
			return kv[1], true
		}
	}

	return "", false
}

// HooksPayloadFromEnv extracts the HooksPayload from the given environment
// variables. If no HooksPayload exists, it returns a ErrPayloadNotFound
// error.
func HooksPayloadFromEnv(envs []string) (HooksPayload, error) {
	encoded, ok := lookupEnv(envs, EnvHooksPayload)
	if !ok {
		return HooksPayload{}, ErrPayloadNotFound
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return HooksPayload{}, err
	}

	var jsonPayload jsonHooksPayload
	if err := json.Unmarshal(decoded, &jsonPayload); err != nil {
		return HooksPayload{}, err
	}

	var repo gitalypb.Repository
	err = protojson.Unmarshal([]byte(jsonPayload.Repo), &repo)
	if err != nil {
		return HooksPayload{}, err
	}

	payload := jsonPayload.HooksPayload
	payload.Repo = &repo

	// If no RequestedHooks are passed down to us, then we need to assume
	// that the caller of this hook isn't aware of this field and thus just
	// pretend that he wants to execute all hooks.
	if payload.RequestedHooks == 0 {
		payload.RequestedHooks = AllHooks
	}

	return payload, nil
}

// Env encodes the given HooksPayload into an environment variable.
func (p HooksPayload) Env() (string, error) {
	repo, err := protojson.Marshal(p.Repo)
	if err != nil {
		return "", err
	}

	jsonPayload := jsonHooksPayload{HooksPayload: p, Repo: string(repo)}
	marshalled, err := json.Marshal(jsonPayload)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(marshalled)

	return fmt.Sprintf("%s=%s", EnvHooksPayload, encoded), nil
}

// IsHookRequested returns whether the HooksPayload is requesting execution of
// the given git hook.
func (p HooksPayload) IsHookRequested(hook Hook) bool {
	return p.RequestedHooks&hook != 0
}
