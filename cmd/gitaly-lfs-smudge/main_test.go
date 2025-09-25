package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/command"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/smudge"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

type capturedOutput struct {
	log.Logger
	output *bytes.Buffer
}

func (c capturedOutput) Output() io.Writer {
	return c.output
}

func TestGitalyLFSSmudge(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	binary := testcfg.BuildGitalyLFSSmudge(t, cfg)

	opts := defaultOptions(t)

	gitlabCfg, cleanup := runTestServer(t, opts)
	defer cleanup()

	tlsCfg := config.TLS{
		CertPath: opts.ServerCertificate.CertPath,
		KeyPath:  opts.ServerCertificate.KeyPath,
	}

	marshalledGitlabCfg, err := json.Marshal(gitlabCfg)
	require.NoError(t, err)

	marshalledTLSCfg, err := json.Marshal(tlsCfg)
	require.NoError(t, err)

	standardEnv := func() []string {
		return []string{
			"GL_REPOSITORY=project-1",
			"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
			"GITALY_TLS=" + string(marshalledTLSCfg),
		}
	}

	for _, tc := range []struct {
		desc               string
		setup              func(t *testing.T) []string
		noLogConfiguration bool
		stdin              io.Reader
		expectedErr        string
		expectedStdout     string
		expectedStderr     string
		expectedLogRegexp  string
	}{
		{
			desc: "success",
			setup: func(t *testing.T) []string {
				return standardEnv()
			},
			stdin:             strings.NewReader(lfsPointer),
			expectedStdout:    "hello world",
			expectedLogRegexp: "Finished HTTP request",
		},
		{
			desc: "success with single envvar",
			setup: func(t *testing.T) []string {
				cfg := smudge.Config{
					GlRepository: "project-1",
					Gitlab:       gitlabCfg,
					TLS:          tlsCfg,
				}

				env, err := cfg.Environment()
				require.NoError(t, err)

				return []string{env}
			},
			stdin:             strings.NewReader(lfsPointer),
			expectedStdout:    "hello world",
			expectedLogRegexp: "Finished HTTP request",
		},
		{
			desc: "missing Gitlab repository",
			setup: func(t *testing.T) []string {
				return []string{
					"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
					"GITALY_TLS=" + string(marshalledTLSCfg),
				}
			},
			stdin:             strings.NewReader(lfsPointer),
			expectedErr:       "exit status 1",
			expectedLogRegexp: "error loading project: GL_REPOSITORY is not defined",
		},
		{
			desc: "missing Gitlab configuration",
			setup: func(t *testing.T) []string {
				return []string{
					"GL_REPOSITORY=project-1",
					"GITALY_TLS=" + string(marshalledTLSCfg),
				}
			},
			stdin:             strings.NewReader(lfsPointer),
			expectedErr:       "exit status 1",
			expectedLogRegexp: "unable to retrieve GL_INTERNAL_CONFIG",
		},
		{
			desc: "missing TLS configuration",
			setup: func(t *testing.T) []string {
				return []string{
					"GL_REPOSITORY=project-1",
					"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
				}
			},
			stdin:             strings.NewReader(lfsPointer),
			expectedErr:       "exit status 1",
			expectedLogRegexp: "unable to retrieve GITALY_TLS",
		},
		{
			desc:               "missing log configuration",
			noLogConfiguration: true,
			setup: func(t *testing.T) []string {
				return []string{
					"GL_REPOSITORY=project-1",
					"GL_INTERNAL_CONFIG=" + string(marshalledGitlabCfg),
					"GITALY_TLS=" + string(marshalledTLSCfg),
				}
			},
			stdin:          strings.NewReader(lfsPointer),
			expectedErr:    "exit status 1",
			expectedStderr: `new subprocess logger: "unmarshal: unexpected end of JSON input"`,
		},
		{
			desc: "missing stdin",
			setup: func(t *testing.T) []string {
				return standardEnv()
			},
			expectedErr:    "exit status 1",
			expectedStdout: "Cannot read from STDIN. This command should be run by the Git 'smudge' filter\n",
		},
		{
			desc: "non-LFS-pointer input",
			setup: func(t *testing.T) []string {
				return standardEnv()
			},
			stdin:             strings.NewReader("somethingsomething"),
			expectedStdout:    "somethingsomething",
			expectedLogRegexp: "^$",
		},
		{
			desc: "mixed input",
			setup: func(t *testing.T) []string {
				return standardEnv()
			},
			stdin:             strings.NewReader(lfsPointer + "\nsomethingsomething\n"),
			expectedStdout:    lfsPointer + "\nsomethingsomething\n",
			expectedLogRegexp: "^$",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			env := tc.setup(t)

			var stdout, stderr, logOutput bytes.Buffer
			opts := []command.Option{
				command.WithStdin(tc.stdin),
				command.WithStdout(&stdout),
				command.WithStderr(&stderr),
				command.WithEnvironment(env),
			}

			if !tc.noLogConfiguration {
				opts = append(opts, command.WithSubprocessLogger(cfg.Logging.Config))
			}

			cmd, err := command.New(ctx, capturedOutput{
				Logger: testhelper.SharedLogger(t),
				output: &logOutput,
			}, []string{binary}, opts...)
			require.NoError(t, err)

			err = cmd.Wait()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedStdout, stdout.String())
			require.Equal(t, tc.expectedStderr, stderr.String())

			if tc.expectedLogRegexp == "" {
				require.Empty(t, logOutput.String())
			} else {
				logData := logOutput.String()
				require.Regexp(t, tc.expectedLogRegexp, logData)
			}
		})
	}
}
