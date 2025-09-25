package stats

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
)

func TestPerformHTTPPush(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	_, targetRepoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	serverPort := gittest.HTTPServer(t, ctx, gitCmdFactory, targetRepoPath, nil)
	url := fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(targetRepoPath))

	for _, tc := range []struct {
		desc            string
		preparePush     func(t *testing.T, cfg config.Cfg) ([]PushCommand, io.Reader)
		expectedErr     error
		expectedTimings []string
		expectedStats   HTTPSendPack
	}{
		{
			desc: "single revision",
			preparePush: func(t *testing.T, cfg config.Cfg) ([]PushCommand, io.Reader) {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				commit := gittest.WriteCommit(t, cfg, repoPath)
				revisions := strings.NewReader(commit.String())
				pack := gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: revisions},
					"-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q",
				)

				return []PushCommand{
					{OldOID: gittest.DefaultObjectHash.ZeroOID, NewOID: commit, Reference: "refs/heads/foobar"},
				}, bytes.NewReader(pack)
			},
			expectedTimings: []string{
				"start", "header", "pack-sideband", "unpack-ok", "response-body", "end",
			},
			expectedStats: HTTPSendPack{
				stats: SendPack{
					updatedRefs:       1,
					packets:           2,
					largestPacketSize: 44,
					multiband: map[string]*bandInfo{
						"pack": {
							packets: 1,
							size:    44,
						},
						"progress": {},
						"error":    {},
					},
				},
			},
		},
		{
			desc: "many revisions",
			preparePush: func(t *testing.T, cfg config.Cfg) ([]PushCommand, io.Reader) {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				commands := make([]PushCommand, 1000)
				commit := gittest.WriteCommit(t, cfg, repoPath)

				for i := 0; i < len(commands); i++ {
					commands[i] = PushCommand{
						OldOID:    gittest.DefaultObjectHash.ZeroOID,
						NewOID:    commit,
						Reference: git.ReferenceName(fmt.Sprintf("refs/heads/branch-%d", i)),
					}
				}

				revisions := strings.NewReader(strings.Repeat(commit.String()+"\n", 1000))
				pack := gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: revisions},
					"-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q",
				)

				return commands, bytes.NewReader(pack)
			},
			expectedTimings: []string{
				"start", "header", "pack-sideband", "unpack-ok", "response-body", "end",
			},
			expectedStats: HTTPSendPack{
				stats: SendPack{
					updatedRefs:       1000,
					packets:           2,
					largestPacketSize: 28909,
					multiband: map[string]*bandInfo{
						"pack": {
							packets: 1,
							size:    28909,
						},
						"progress": {},
						"error":    {},
					},
				},
			},
		},
		{
			desc: "branch deletion",
			preparePush: func(t *testing.T, cfg config.Cfg) ([]PushCommand, io.Reader) {
				oldOID := gittest.WriteCommit(t, cfg, targetRepoPath, gittest.WithBranch("feature"))

				return []PushCommand{
					{OldOID: oldOID, NewOID: gittest.DefaultObjectHash.ZeroOID, Reference: "refs/heads/feature"},
				}, nil
			},
			expectedTimings: []string{
				"start", "header", "pack-sideband", "unpack-ok", "response-body", "end",
			},
			expectedStats: HTTPSendPack{
				stats: SendPack{
					updatedRefs:       1,
					packets:           2,
					largestPacketSize: 45,
					multiband: map[string]*bandInfo{
						"pack": {
							packets: 1,
							size:    45,
						},
						"progress": {},
						"error":    {},
					},
				},
			},
		},
		{
			desc: "failing delete",
			preparePush: func(t *testing.T, cfg config.Cfg) ([]PushCommand, io.Reader) {
				gittest.WriteCommit(t, cfg, targetRepoPath, gittest.WithBranch(git.DefaultBranch))

				oldOID := git.ObjectID(strings.Repeat("1", gittest.DefaultObjectHash.EncodedLen()))

				return []PushCommand{
					{OldOID: oldOID, NewOID: gittest.DefaultObjectHash.ZeroOID, Reference: git.DefaultRef},
				}, nil
			},
			expectedErr: fmt.Errorf("parsing packfile response: %w",
				errors.New("reference update failed: \"ng "+string(git.DefaultRef)+" deletion of the current branch prohibited\\n\"")),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			commands, packfile := tc.preparePush(t, cfg)

			start := time.Now()

			stats, err := PerformHTTPPush(ctx, url, "", "", gittest.DefaultObjectHash, commands, packfile, false)
			require.Equal(t, tc.expectedErr, err)
			if err != nil {
				return
			}

			end := time.Now()

			timings := map[string]*time.Time{
				"start":         &stats.SendPack.start,
				"header":        &stats.SendPack.header,
				"pack-sideband": &stats.SendPack.stats.multiband["pack"].firstPacket,
				"unpack-ok":     &stats.SendPack.stats.unpackOK,
				"response-body": &stats.SendPack.stats.responseBody,
				"end":           &end,
			}

			previousTime := start
			for _, expectedTiming := range tc.expectedTimings {
				timing := timings[expectedTiming]
				require.GreaterOrEqual(t, *timing, previousTime,
					"expected to receive %q packet before before %q, but received at %q",
					expectedTiming, previousTime, timing,
				)
				previousTime = *timing
				*timing = time.Time{}
			}

			stats.SendPack.stats.ReportProgress = nil
			require.Equal(t, tc.expectedStats, stats.SendPack)
		})
	}
}
