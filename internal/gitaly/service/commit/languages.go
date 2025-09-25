package commit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var errAmbigRef = errors.New("ambiguous reference")

func (s *server) validateCommitLanguagesRequest(ctx context.Context, req *gitalypb.CommitLanguagesRequest) error {
	if err := s.locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return err
	}
	if err := git.ValidateRevision(req.GetRevision(), git.AllowEmptyRevision()); err != nil {
		return err
	}
	return nil
}

func (s *server) CommitLanguages(ctx context.Context, req *gitalypb.CommitLanguagesRequest) (*gitalypb.CommitLanguagesResponse, error) {
	if err := s.validateCommitLanguagesRequest(ctx, req); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	repo := s.localRepoFactory.Build(req.GetRepository())

	revision := string(req.GetRevision())
	if revision == "" {
		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return nil, err
		}
		revision = defaultBranch.String()
	}

	commitID, err := s.lookupRevision(ctx, repo, revision)
	if err != nil {
		return nil, structerr.NewInternal("looking up revision: %w", err)
	}

	stats, err := linguist.New(s.cfg, s.logger, s.catfileCache, repo).Stats(ctx, git.ObjectID(commitID))
	if err != nil {
		return nil, structerr.NewInternal("language stats: %w", err)
	}

	resp := &gitalypb.CommitLanguagesResponse{}
	if len(stats) == 0 {
		return resp, nil
	}

	total := uint64(0)
	for _, count := range stats {
		total += count
	}

	if total == 0 {
		return nil, structerr.NewInternal("linguist stats added up to zero: %v", stats)
	}

	for lang, count := range stats {
		languageID, err := linguist.LanguageID(lang)
		if err != nil {
			return nil, structerr.NewInternal("linguist language_id fetch: %w", err)
		}

		l := &gitalypb.CommitLanguagesResponse_Language{
			Name:       lang,
			Share:      float32(100*count) / float32(total),
			Color:      linguist.Color(lang),
			LanguageId: int64(languageID),
			Bytes:      count,
		}
		resp.Languages = append(resp.Languages, l)
	}

	sort.Sort(languageSorter(resp.GetLanguages()))

	return resp, nil
}

type languageSorter []*gitalypb.CommitLanguagesResponse_Language

func (ls languageSorter) Len() int           { return len(ls) }
func (ls languageSorter) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls languageSorter) Less(i, j int) bool { return ls[i].GetShare() > ls[j].GetShare() }

func (s *server) lookupRevision(ctx context.Context, repo gitcmd.RepositoryExecutor, revision string) (string, error) {
	rev, err := s.checkRevision(ctx, repo, revision)
	if err != nil {
		if errors.Is(err, errAmbigRef) {
			fullRev, err := s.disambiguateRevision(ctx, repo, revision)
			if err != nil {
				return "", err
			}

			rev, err = s.checkRevision(ctx, repo, fullRev)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	return rev, nil
}

func (s *server) checkRevision(ctx context.Context, repo gitcmd.RepositoryExecutor, revision string) (string, error) {
	var stdout, stderr bytes.Buffer

	revParse, err := repo.Exec(ctx,
		gitcmd.Command{Name: "rev-parse", Args: []string{revision}},
		gitcmd.WithStdout(&stdout),
		gitcmd.WithStderr(&stderr),
	)
	if err != nil {
		return "", err
	}

	if err = revParse.Wait(); err != nil {
		errMsg := strings.Split(stderr.String(), "\n")[0]
		return "", fmt.Errorf("%w: %v", err, errMsg)
	}

	if strings.HasSuffix(stderr.String(), "refname '"+revision+"' is ambiguous.\n") {
		return "", errAmbigRef
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

func (s *server) disambiguateRevision(ctx context.Context, repo gitcmd.RepositoryExecutor, revision string) (string, error) {
	cmd, err := repo.Exec(ctx, gitcmd.Command{
		Name:  "for-each-ref",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--format=%(refname)"}},
		Args:  []string{"**/" + revision},
	}, gitcmd.WithSetupStdout())
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		refName := scanner.Text()

		if strings.HasPrefix(refName, "refs/heads") {
			return refName, nil
		}
	}

	return "", fmt.Errorf("branch %v not found", revision)
}
