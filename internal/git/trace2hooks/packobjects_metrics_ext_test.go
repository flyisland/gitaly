package trace2hooks_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2hooks"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestPackObjectsMetrics(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc      string
		setupRepo func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer)
		assert    func(*testing.T, log.Fields)
	}{
		{
			desc: "pack blobs",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				for i := 0; i <= 10; i++ {
					content := fmt.Sprintf("hello %d", i)
					input.WriteString(gittest.WriteBlob(t, cfg, repoPath, []byte(content)).String())
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 11, statFields["pack_objects.written_object_count"])
				require.Equal(t, 0, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
		{
			desc: "pack commits",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				input.WriteString(gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("reachable"), gittest.WithBranch("reachable")).String())
				input.WriteString("\n")
				input.WriteString(gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("unreachable")).String())
				input.WriteString("\n")

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 3, statFields["pack_objects.written_object_count"])
				require.Equal(t, 1, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
		{
			desc: "pack reuse with bitmap",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				input.WriteString(gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("reachable"), gittest.WithBranch("reachable")).String())
				input.WriteString("\n")
				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Adbl")

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 2, statFields["pack_objects.written_object_count"])
				require.Equal(t, 0, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 2, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 1, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
		{
			desc: "pack refs",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit"), gittest.WithBranch("main"))
				for i := 0; i <= 10; i++ {
					ref := fmt.Sprintf("ref-%d", i)
					gittest.WriteRef(t, cfg, repoPath, git.ReferenceName(ref), commit)
					input.WriteString(ref)
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 2, statFields["pack_objects.written_object_count"])
				require.Equal(t, 0, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
		{
			desc: "pack tags",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("commit"), gittest.WithBranch("main"))
				for i := 0; i <= 10; i++ {
					tag := fmt.Sprintf("tag-%d", i)
					gittest.WriteTag(t, cfg, repoPath, tag, commit.Revision())
					input.WriteString(tag)
					input.WriteString("\n")
				}

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 2, statFields["pack_objects.written_object_count"])
				require.Equal(t, 0, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
		{
			desc: "pack tree",
			setupRepo: func(ctx context.Context) (*gitalypb.Repository, config.Cfg, bytes.Buffer) {
				cfg := testcfg.Build(t)
				repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg,
					gittest.CreateRepositoryConfig{SkipCreationViaService: true},
				)

				var input bytes.Buffer
				tree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
					{
						Mode:    "100644",
						Path:    "file1",
						Content: "file1",
					},
					{
						Mode:    "100644",
						Path:    "file2",
						Content: "file2",
					},
					{
						Mode:    "100644",
						Path:    "file3",
						Content: "file3",
					},
					{
						OID: gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
							{
								Mode:    "100644",
								Path:    "subfile1",
								Content: "subfile1",
							},
							{
								Mode:    "100644",
								Path:    "subfile2",
								Content: "subfile2",
							},
						}),
						Mode: "040000",
						Path: "subdir",
					},
				})
				input.WriteString(tree.String())
				input.WriteString("\n")

				return repoProto, cfg, input
			},
			assert: func(t *testing.T, statFields log.Fields) {
				require.Equal(t, 7, statFields["pack_objects.written_object_count"])
				require.Equal(t, 0, statFields["pack_objects.written_object_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.reused_objects_delta_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_objects_count"])
				require.Equal(t, 0, statFields["pack_objects.pack_reused_packfiles_count"])
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			ctx := log.InitContextCustomFields(testhelper.Context(t))
			repoProto, cfg, input := tc.setupRepo(ctx)
			gitCmdFactory := gittest.NewCommandFactory(t, cfg, gitcmd.WithTrace2Hooks([]trace2.Hook{
				trace2hooks.NewPackObjectsMetrics(),
			}))

			cmd, err := gitCmdFactory.New(ctx, repoProto, gitcmd.Command{
				Name: "pack-objects",
				Flags: []gitcmd.Option{
					gitcmd.Flag{Name: "--compression=0"},
					gitcmd.Flag{Name: "--stdout"},
					gitcmd.Flag{Name: "--unpack-unreachable"},
					gitcmd.Flag{Name: "-q"},
				},
			}, gitcmd.WithStdin(&input))
			require.NoError(t, err)

			err = cmd.Wait()
			require.NoError(t, err)

			customFields := log.CustomFieldsFromContext(ctx)
			require.NotNil(t, customFields)

			logrusFields := customFields.Fields()
			require.Equal(t, "true", logrusFields["trace2.activated"])
			require.Equal(t, "pack_objects_metrics", logrusFields["trace2.hooks"])
			require.Contains(t, logrusFields, "pack_objects.enumerate_objects_ms")
			require.Contains(t, logrusFields, "pack_objects.prepare_pack_ms")
			require.Contains(t, logrusFields, "pack_objects.write_pack_file_ms")

			tc.assert(t, logrusFields)
		})
	}
}
