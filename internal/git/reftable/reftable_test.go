package reftable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
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

func TestParseTable(t *testing.T) {
	t.Parallel()

	if !testhelper.IsReftableEnabled() {
		t.Skip("tests are reftable specific")
	}

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

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

			table, err := ParseTable(reftablePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, table)

			references, err := table.GetReferences()
			require.NoError(t, err)

			require.Equal(t, setup.references, references)
		})
	}
}

func TestParseTable_validation(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skip("This test is reftable specific.")
	}

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	patchHeader := func(t *testing.T, f *os.File, hdr header) {
		buf := bytes.NewBuffer(nil)
		require.NoError(t, binary.Write(buf, binary.BigEndian, hdr.headerV1))

		if hdr.headerV1.Version >= 2 {
			require.NoError(t, binary.Write(buf, binary.BigEndian, hdr.HashID))
		}

		_, err := f.WriteAt(buf.Bytes(), 0)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		desc                 string
		patchTable           func(*testing.T, *os.File, footer)
		expectedErrorMessage string
	}{
		{
			desc: "unexpected magic",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.Magic = [...]byte{'I', 'V', 'A', 'L'}
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unexpected magic bytes: "IVAL"`,
		},
		{
			desc: "unsupported version",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.Version = 3
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unsupported version: 3`,
		},
		{
			desc: "unsupported hash",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				if f.Version < 2 {
					t.Skip("Hash ID is only present on reftable version 2.")
				}

				f.header.HashID = [...]byte{'I', 'V', 'A', 'L'}
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `parse header: unsupported hash id: "IVAL"`,
		},
		{
			desc: "mismatching header and footer",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				f.header.MaxUpdateIndex++
				patchHeader(t, file, f.header)
			},
			expectedErrorMessage: `footer doesn't match header`,
		},
		{
			desc: "invalid checksum",
			patchTable: func(t *testing.T, file *os.File, f footer) {
				// The checksum is at the end of the file. Modify the preceding byte without updating the
				// checksum to trigger a checksumming failure.
				info, err := file.Stat()
				require.NoError(t, err)

				_, err = file.WriteAt([]byte{255}, info.Size()-crc32.Size-1)
				require.NoError(t, err)
			},
			expectedErrorMessage: "parse footer: checksum mismatch",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
				SkipCreationViaService: true,
			})

			tables, err := ReadTablesList(repoPath)
			require.NoError(t, err)

			tablePath := filepath.Join(repoPath, "reftable", tables[0].String())
			table, err := ParseTable(tablePath)
			require.NoError(t, err)
			defer testhelper.MustClose(t, table)

			file, err := os.OpenFile(tablePath, os.O_RDWR, 0)
			require.NoError(t, err)
			defer testhelper.MustClose(t, file)

			tc.patchTable(t, file, table.footer)

			table, err = ParseTable(tablePath)
			require.EqualError(t, err, tc.expectedErrorMessage)
			require.Nil(t, table)
		})
	}
}

func TestTable_updateIndex(t *testing.T) {
	if !testhelper.IsReftableEnabled() {
		t.Skip("This test is reftable specific.")
	}

	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	_, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	// Create a new branch. This creates a table that contains the original
	// HEAD at update index 1 and the new branch at updated index 2.
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch-1"))

	tables, err := ReadTablesList(repoPath)
	require.NoError(t, err)

	table, err := ParseTable(filepath.Join(repoPath, "reftable", tables[0].String()))
	require.NoError(t, err)
	defer testhelper.MustClose(t, table)

	require.Equal(t, uint64(1), table.MinUpdateIndex())
	require.Equal(t, uint64(2), table.MaxUpdateIndex())
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

func TestName_string(t *testing.T) {
	require.Equal(t,
		"0x000000000001-0x0000000005f6-b54f3b59.ref",
		Name{
			MinUpdateIndex: 1,
			MaxUpdateIndex: 1526,
			Suffix:         "b54f3b59",
		}.String(),
	)
}
