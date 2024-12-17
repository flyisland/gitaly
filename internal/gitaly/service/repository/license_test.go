package repository

import (
	"testing"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const (
	mitLicense = `MIT License

Copyright (c) [year] [fullname]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`
)

func TestFindLicense_successful(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryService(t)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc                  string
		nonExistentRepository bool
		setup                 func(t *testing.T, repoPath string)
		expectedLicense       *gitalypb.FindLicenseResponse
		errorContains         string
	}{
		{
			desc: "repository does not exist",
			setup: func(t *testing.T, repoPath string) {
			},
			nonExistentRepository: true,
			errorContains:         storage.ErrRepositoryNotFound.Error(),
		},
		{
			desc: "empty if no license file in repo",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "README.md",
							Content: "readme content",
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{},
		},
		{
			desc: "high confidence mit result and less confident mit-0 result",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "LICENSE",
							Content: mitLicense,
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "mit",
				LicenseUrl:       "https://opensource.org/licenses/MIT",
				LicenseName:      "MIT License",
				LicensePath:      "LICENSE",
			},
		},
		{
			// test for https://gitlab.com/gitlab-org/gitaly/-/issues/4745
			desc: "ignores licenses that don't have further details",
			setup: func(t *testing.T, repoPath string) {
				licenseText := testhelper.MustReadFile(t, "testdata/linux-license.txt")

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "COPYING",
							Content: string(licenseText),
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "gpl-2.0+",
				LicenseName:      "GNU General Public License v2.0 or later",
				LicenseUrl:       "https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html",
				LicensePath:      "COPYING",
			},
		},
		{
			desc: "unknown license",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "LICENSE.md",
							Content: "this doesn't match any known license",
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "other",
				LicenseName:      "Other",
				LicenseNickname:  "LICENSE",
				LicensePath:      "LICENSE.md",
			},
		},
		{
			desc: "deprecated license",
			setup: func(t *testing.T, repoPath string) {
				deprecatedLicenseData := testhelper.MustReadFile(t, "testdata/gnu_license.deprecated.txt")

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "LICENSE",
							Content: string(deprecatedLicenseData),
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "gpl-3.0+",
				LicenseUrl:       "https://www.gnu.org/licenses/gpl-3.0-standalone.html",
				LicenseName:      "GNU General Public License v3.0 or later",
				LicensePath:      "LICENSE",
				// The nickname is not set because there is no nickname defined for gpl-3.0+ license.
			},
		},
		{
			desc: "license with nickname",
			setup: func(t *testing.T, repoPath string) {
				licenseText := testhelper.MustReadFile(t, "testdata/gpl-2.0_license.txt")

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "LICENSE",
							Content: string(licenseText),
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "gpl-2.0",
				LicenseUrl:       "https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html",
				LicenseName:      "GNU General Public License v2.0 only",
				LicensePath:      "LICENSE",
				LicenseNickname:  "GNU GPLv2",
			},
		},
		{
			desc: "license in subdir",
			setup: func(t *testing.T, repoPath string) {
				subTree := gittest.WriteTree(t, cfg, repoPath,
					[]gittest.TreeEntry{{
						Mode:    "100644",
						Path:    "LICENSE",
						Content: mitLicense,
					}})

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode: "040000",
							Path: "legal",
							OID:  subTree,
						}))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{},
		},
		{
			desc: "license pointing to license file",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "mit.txt",
							Content: mitLicense,
						},
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "LICENSE",
							Content: "mit.txt",
						},
					))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{
				LicenseShortName: "mit",
				LicenseUrl:       "https://opensource.org/licenses/MIT",
				LicenseName:      "MIT License",
				LicensePath:      "mit.txt",
			},
		},
		{
			desc: "license in README",
			setup: func(t *testing.T, repoPath string) {
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"),
					gittest.WithTreeEntries(
						gittest.TreeEntry{
							Mode:    "100644",
							Path:    "README",
							Content: "This project is released under MIT license",
						},
					))
			},
			expectedLicense: &gitalypb.FindLicenseResponse{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath := gittest.CreateRepository(t, ctx, cfg)
			tc.setup(t, repoPath)

			if tc.nonExistentRepository {
				_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
				require.NoError(t, err)
			}

			existResp, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: repo})
			require.NoError(t, err)

			if existResp.GetExists() {
				_, err = client.WriteRef(ctx, &gitalypb.WriteRefRequest{
					Repository: repo,
					Ref:        []byte("HEAD"),
					Revision:   []byte("refs/heads/main"),
				})
				require.NoError(t, err)
			}

			resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
			if tc.errorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorContains)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, tc.expectedLicense, resp)
		})
	}
}

func TestFindLicense_emptyRepo(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryService(t)
	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(t, ctx, cfg)

	resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
	require.NoError(t, err)

	require.Empty(t, resp.GetLicenseShortName())
}

func TestFindLicense_validate(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	client, serverSocketPath := runRepositoryService(t, cfg)
	cfg.SocketPath = serverSocketPath
	_, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: nil})
	testhelper.RequireGrpcError(t, structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet), err)
}

func BenchmarkFindLicense(b *testing.B) {
	cfg := testcfg.Build(b)
	ctx := testhelper.Context(b)

	gitCmdFactory := gittest.NewCountingCommandFactory(b, cfg)
	client, serverSocketPath := runRepositoryService(
		b,
		cfg,
		testserver.WithGitCommandFactory(gitCmdFactory),
	)
	cfg.SocketPath = serverSocketPath

	// Warm up the license database
	licensedb.Preload()

	repoGitLab, _ := gittest.CreateRepository(b, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
		Seed:                   "benchmark.git",
	})

	repoStress, repoStressPath := gittest.CreateRepository(b, ctx, cfg)

	// Based on https://github.com/go-enry/go-license-detector/blob/18a439e5437cd46905b074ac24c27cbb6cac4347/licensedb/internal/investigation.go#L28-L38
	fileNames := []string{
		"licence",
		"lisence", //nolint:misspell
		"lisense", //nolint:misspell
		"license",
		"licences",
		"lisences",
		"lisenses",
		"licenses",
		"legal",
		"copyleft",
		"copyright",
		"copying",
		"unlicense",
		"gpl-v1",
		"gpl-v2",
		"gpl-v3",
		"lgpl-v1",
		"lgpl-v2",
		"lgpl-v3",
		"bsd",
		"mit",
		"apache",
	}
	fileExtensions := []string{
		"",
		".md",
		".rst",
		".html",
		".txt",
	}

	treeEntries := make([]gittest.TreeEntry, 0, len(fileNames)*len(fileExtensions))

	for _, name := range fileNames {
		for _, ext := range fileExtensions {
			treeEntries = append(treeEntries,
				gittest.TreeEntry{
					Mode:    "100644",
					Path:    name + ext,
					Content: mitLicense + "\n" + name, // grain of salt
				})
		}
	}

	gittest.WriteCommit(b, cfg, repoStressPath, gittest.WithBranch("main"),
		gittest.WithTreeEntries(treeEntries...))
	gittest.Exec(b, cfg, "-C", repoStressPath, "symbolic-ref", "HEAD", "refs/heads/main")

	for _, tc := range []struct {
		desc string
		repo *gitalypb.Repository
	}{
		{
			desc: "gitlab-org/gitlab.git",
			repo: repoGitLab,
		},
		{
			desc: "stress.git",
			repo: repoStress,
		},
	} {
		// Preheat
		_, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: tc.repo})
		require.NoError(b, err)
		gitCmdFactory.ResetCount()

		b.Run(tc.desc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: tc.repo})
				require.NoError(b, err)
				require.Equal(b, "mit", resp.GetLicenseShortName())
			}

			gitCmdFactory.RequireCommandCount(b, "cat-file", 0)
		})
	}
}
