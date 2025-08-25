package commit

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const (
	rubyContent = "puts 'Hello world!'\n"

	javaContent = `public class HelloWorldFactory {
	public static void main(String[] args){
		IHelloWorldFactory factory = HelloWorldFactory().getInstance();
		IHelloWorld helloWorld = factory.getHelloWorld();
		IHelloWorldString helloWorldString = helloWorld.getHelloWorldString();
		IPrintStrategy printStrategy = helloWorld.getPrintStrategy();
		helloWorld.print(printStrategy, helloWorldString%);
	}
}
`

	cContent = `#include <stdio.h>

	int main(const char *argv[]) {
		puts("Hello, world!")
	}
`
)

func TestCommitLanguages(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	cfg.SocketPath = startTestServices(t, cfg)
	client := newCommitServiceClient(t, cfg.SocketPath)

	type setupData struct {
		request          *gitalypb.CommitLanguagesRequest
		expectedErr      error
		expectedResponse *gitalypb.CommitLanguagesResponse
	}

	for _, tc := range []struct {
		desc  string
		setup func(t *testing.T) setupData
	}{
		{
			desc: "no repository provided",
			setup: func(t *testing.T) setupData {
				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: nil,
					},
					expectedErr: structerr.NewInvalidArgument("%w", storage.ErrRepositoryNotSet),
				}
			},
		},
		{
			desc: "invalid revision",
			setup: func(t *testing.T) setupData {
				repo, _ := gittest.CreateRepository(t, ctx, cfg)

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte("--output=/meow"),
					},
					expectedErr: structerr.NewInvalidArgument("revision can't start with '-'"),
				}
			},
		},
		{
			desc: "empty revision falls back to default branch",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   nil,
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "C", Color: "#555555", Share: 100, Bytes: uint64(len(cContent)), LanguageId: 41},
						},
					},
				}
			},
		},
		{
			desc: "ambiguous revision",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				// Write both a branch and a tag named "v1.0.0" with different contents so that we can
				// verify which of both references has precedence.
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("v1.0.0"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))
				gittest.WriteCommit(t, cfg, repoPath, gittest.WithReference("refs/tags/v1.0.0"), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.rb", Mode: "100644", Content: rubyContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte("v1.0.0"),
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "C", Color: "#555555", Share: 100, Bytes: uint64(len(cContent)), LanguageId: 41},
						},
					},
				}
			},
		},
		{
			desc: "multiple languages",
			setup: func(t *testing.T) setupData {
				repo, repoPath := gittest.CreateRepository(t, ctx, cfg)

				commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithTreeEntries(
					gittest.TreeEntry{Path: "file.rb", Mode: "100644", Content: rubyContent},
					gittest.TreeEntry{Path: "file.java", Mode: "100644", Content: javaContent},
					gittest.TreeEntry{Path: "file.c", Mode: "100644", Content: cContent},
				))

				return setupData{
					request: &gitalypb.CommitLanguagesRequest{
						Repository: repo,
						Revision:   []byte(commit),
					},
					expectedResponse: &gitalypb.CommitLanguagesResponse{
						Languages: []*gitalypb.CommitLanguagesResponse_Language{
							{Name: "Java", Color: "#b07219", Share: 79.67145538330078, Bytes: uint64(len(javaContent)), LanguageId: 181},
							{Name: "C", Color: "#555555", Share: 16.221765518188477, Bytes: uint64(len(cContent)), LanguageId: 41},
							{Name: "Ruby", Color: "#701516", Share: 4.106776237487793, Bytes: uint64(len(rubyContent)), LanguageId: 326},
						},
					},
				}
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			setup := tc.setup(t)

			response, err := client.CommitLanguages(ctx, setup.request)
			testhelper.RequireGrpcError(t, setup.expectedErr, err)
			testhelper.ProtoEqual(t, setup.expectedResponse, response)
		})
	}
}
