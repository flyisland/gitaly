package reftable

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func getReftables(repoPath string) []string {
	tables := []string{}

	reftablePath := filepath.Join(repoPath, "reftable")

	files, err := os.ReadDir(reftablePath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if filepath.Base(file.Name()) == "tables.list" {
			continue
		}

		tables = append(tables, filepath.Join(reftablePath, file.Name()))
	}

	return tables
}

func TestParseReftable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	tableName := [4]byte{}
	n, err := strings.NewReader("REFT").Read(tableName[:])
	require.NoError(t, err)
	require.Equal(t, 4, n)

	type setupData struct {
		repoPath   string
		references []git.Reference
	}

	for _, tc := range []struct {
		name        string
		setup       func() setupData
		expectedErr error
	}{
		{
			name: "single ref",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "single ref + annotated tag",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				annotatedTag := gittest.WriteTag(t, cfg, repoPath, "v2.0.0", mainCommit.Revision(), gittest.WriteTagConfig{
					Message: "annotated tag",
				})

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/tags/v2.0.0",
							Target: annotatedTag.String(),
						},
					},
				}
			},
		},
		{
			name: "two refs without prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				rootRefCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("ROOTREF"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "ROOTREF",
							Target: rootRefCommit.String(),
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "two refs with prefix compression",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "multiple refs with different commit IDs",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				mainCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"))
				masterCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(mainCommit), gittest.WithBranch("master"))

				return setupData{
					repoPath: repoPath,
					references: []git.Reference{
						{
							Name:       "HEAD",
							Target:     "refs/heads/main",
							IsSymbolic: true,
						},
						{
							Name:   "refs/heads/main",
							Target: mainCommit.String(),
						},
						{
							Name:   "refs/heads/master",
							Target: masterCommit.String(),
						},
					},
				}
			},
		},
		{
			name: "multiple blocks in table",
			setup: func() setupData {
				_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
					SkipCreationViaService: true,
				})

				var references []git.Reference

				references = append(references, git.Reference{
					Name:       "HEAD",
					Target:     "refs/heads/main",
					IsSymbolic: true,
				})

				for i := 0; i < 100; i++ {
					branch := fmt.Sprintf("branch%02d", i)
					references = append(references, git.Reference{
						Name:   git.ReferenceName(fmt.Sprintf("refs/heads/%s", branch)),
						Target: gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(branch)).String(),
					})
				}

				return setupData{
					repoPath:   repoPath,
					references: references,
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup()

			repoPath := setup.repoPath

			// pack-refs so there is only one table
			gittest.Exec(t, cfg, "-C", repoPath, "pack-refs")
			reftablePath := getReftables(repoPath)[0]

			file, err := os.Open(reftablePath)
			require.NoError(t, err)
			defer file.Close()

			buf, err := io.ReadAll(file)
			require.NoError(t, err)

			table, err := NewReftable(buf)
			require.NoError(t, err)

			references, err := table.IterateRefs()
			require.NoError(t, err)

			require.Equal(t, setup.references, references)
		})
	}
}

func TestParseName(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc             string
		reftableName     string
		expectedName     Name
		expectedErrorStr string
	}{
		{
			desc:             "invalid table name",
			reftableName:     "spy-from-files-backend.ref",
			expectedErrorStr: "reftable name \"spy-from-files-backend.ref\" malformed",
		},
		{
			desc:             "non hex values in min index",
			reftableName:     "0xzz0000000001-0x00000000000a-b54f3b59.ref",
			expectedErrorStr: "reftable name \"0xzz0000000001-0x00000000000a-b54f3b59.ref\" malformed",
		},
		{
			desc:             "non hex values in max index",
			reftableName:     "0x000000000001-0x0000000000za-b54f3b59.ref",
			expectedErrorStr: "reftable name \"0x000000000001-0x0000000000za-b54f3b59.ref\" malformed",
		},
		{
			desc:         "small values in table name",
			reftableName: "0x000000000001-0x000000000005-b54f3b59.ref",
			expectedName: Name{
				MinUpdateIndex: 1,
				MaxUpdateIndex: 5,
				Suffix:         "b54f3b59",
			},
		},
		{
			desc:         "medium values in table name",
			reftableName: "0x0000000000af-0x0000000000ee-ab413b60.ref",
			expectedName: Name{
				MinUpdateIndex: 175,
				MaxUpdateIndex: 238,
				Suffix:         "ab413b60",
			},
		},
		{
			desc:         "upper case hex in table name",
			reftableName: "0x0000000000DE-0x0000000000AD-c23a3459.ref",
			expectedName: Name{
				MinUpdateIndex: 222,
				MaxUpdateIndex: 173,
				Suffix:         "c23a3459",
			},
		},
		{
			desc:         "max values in table name",
			reftableName: "0xeeeeeeeeeeee-0xeeeeeeeeeeee-ddff2b19.ref",
			expectedName: Name{
				MinUpdateIndex: 262709978263278,
				MaxUpdateIndex: 262709978263278,
				Suffix:         "ddff2b19",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			actualName, err := ParseName(tc.reftableName)
			if tc.expectedErrorStr != "" {
				require.EqualError(t, err, tc.expectedErrorStr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, actualName)
		})
	}
}
