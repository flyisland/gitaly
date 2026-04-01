package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/go-enry/go-license-detector/v4/licensedb/api"
	"github.com/go-enry/go-license-detector/v4/licensedb/filer"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/tracing"
	"gitlab.com/gitlab-org/gitaly/v18/internal/unarycache"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

// The `github.com/go-enry/go-license-detector` package uses https://spdx.org/licenses/
// as the source of the licenses. That package doesn't provide `nickname` info.
// But because the `nickname` is required by the FindLicense RPC interface, we had to manually
// extract the list of all license-to-nickname pairs from the Licensee license
// database which is https://github.com/github/choosealicense.com/tree/gh-pages/_licenses
// and store them here.
var nicknameByLicenseIdentifier = map[string]string{
	"agpl-3.0":           "GNU AGPLv3",
	"lgpl-3.0":           "GNU LGPLv3",
	"bsd-3-clause-clear": "Clear BSD",
	"odbl-1.0":           "ODbL",
	"ncsa":               "UIUC/NCSA",
	"lgpl-2.1":           "GNU LGPLv2.1",
	"gpl-3.0":            "GNU GPLv3",
	"gpl-2.0":            "GNU GPLv2",
}

func newLicenseCache() *unarycache.Cache[git.ObjectID, *gitalypb.FindLicenseResponse] {
	cache, err := unarycache.New(100, findLicense)
	if err != nil {
		panic(err)
	}
	return cache
}

func (s *server) FindLicense(ctx context.Context, req *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	repository := req.GetRepository()
	if err := s.locator.ValidateRepository(ctx, repository); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}
	repo := s.localRepoFactory.Build(repository)

	headOID, err := repo.ResolveRevision(ctx, "HEAD")
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return &gitalypb.FindLicenseResponse{}, nil
		}
		return nil, structerr.NewInternal("cannot find HEAD revision: %w", err)
	}

	response, err := s.licenseCache.GetOrCompute(ctx, repo, headOID)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func findLicense(ctx context.Context, repo *localrepo.Repo, commitID git.ObjectID) (*gitalypb.FindLicenseResponse, error) {
	span, ctx := tracing.StartSpanIfHasParent(ctx, "repository.findLicense", nil)
	defer span.End()

	repoFiler := &gitFiler{ctx: ctx, repo: repo, treeishID: commitID}
	detectedLicenses, err := licensedb.Detect(repoFiler)
	if err != nil {
		if errors.Is(err, licensedb.ErrNoLicenseFound) {
			if repoFiler.foundLicense {
				// In case the license is not identified, but a file containing some
				// sort of license is found, we return a predefined response.
				return &gitalypb.FindLicenseResponse{
					LicenseName:      "Other",
					LicenseShortName: "other",
					LicenseNickname:  "LICENSE", // Show as LICENSE in the UI
					LicensePath:      repoFiler.path,
				}, nil
			}
			return &gitalypb.FindLicenseResponse{}, nil
		}
		return nil, structerr.NewInternal("detect licenses: %w", err)
	}

	type bestMatch struct {
		shortName string
		api.Match
	}
	bestMatches := make([]bestMatch, 0, len(detectedLicenses))
	for candidate, match := range detectedLicenses {
		_, err := licensedb.LicenseName(trimDeprecatedPrefix(candidate))
		if err != nil {
			if errors.Is(err, licensedb.ErrUnknownLicenseID) {
				continue
			}
			return nil, structerr.NewInternal("license name by id %q: %w", candidate, err)
		}
		bestMatches = append(bestMatches, bestMatch{Match: match, shortName: candidate})
	}

	if len(bestMatches) == 0 {
		return &gitalypb.FindLicenseResponse{}, nil
	}

	sort.Slice(bestMatches, func(i, j int) bool {
		iCanonical := isCanonicalLicenseFile(bestMatches[i].File)
		jCanonical := isCanonicalLicenseFile(bestMatches[j].File)

		// When one match comes from a canonical license file and the other
		// from a variant (e.g., LICENSE-3RD-PARTY.md), prefer the canonical
		// file. This prevents third-party notice files from overriding the
		// actual project license even when they score higher confidence.
		if iCanonical != jCanonical {
			return iCanonical
		}

		// Among matches of the same canonical/variant status, sort by
		// confidence descending.
		if bestMatches[i].Confidence != bestMatches[j].Confidence {
			return bestMatches[i].Confidence > bestMatches[j].Confidence
		}

		// Tiebreaker: alphabetical by short name for deterministic results.
		return trimDeprecatedPrefix(bestMatches[i].shortName) < trimDeprecatedPrefix(bestMatches[j].shortName)
	})

	// We also don't want to return the prefix back to the caller if it exists.
	shortName := trimDeprecatedPrefix(bestMatches[0].shortName)

	name, err := licensedb.LicenseName(shortName)
	if err != nil {
		return nil, structerr.NewInternal("license name by id %q: %w", shortName, err)
	}

	urls, err := licensedb.LicenseURLs(shortName)
	if err != nil {
		return nil, structerr.NewInternal("license URLs by id %q: %w", shortName, err)
	}
	var url string
	if len(urls) > 0 {
		// The URL list is returned in an ordered slice, so we just pick up the first one from the list.
		url = urls[0]
	}

	// The license identifier used by `github.com/go-enry/go-license-detector` is
	// case-sensitive, but the API requires all license identifiers to be lower-cased.
	shortName = strings.ToLower(shortName)
	nickname := nicknameByLicenseIdentifier[shortName]
	return &gitalypb.FindLicenseResponse{
		LicenseShortName: shortName,
		LicensePath:      bestMatches[0].File,
		LicenseName:      name,
		LicenseUrl:       url,
		LicenseNickname:  nickname,
	}, nil
}

// For the deprecated licenses, the `github.com/go-enry/go-license-detector` package
// uses the "deprecated_" prefix in the identifier. But the license database stores
// information using the identifier without prefix, so we need to cut off the
// prefix before searching for full license name and license URLs.
func trimDeprecatedPrefix(name string) string {
	return strings.TrimPrefix(name, "deprecated_")
}

var readmeRegexp = regexp.MustCompile(`(readme|guidelines)(\.md|\.rst|\.html|\.txt)?$`)

// canonicalLicenseFileRe matches canonical license filenames (LICENSE, LICENCE,
// COPYING, UNLICENSE) with no suffix or a common doc extension. Used during
// result sorting to prioritize matches from canonical license files over
// variant files like LICENSE-MIT or LICENSE-3RD-PARTY.md.
var canonicalLicenseFileRe = regexp.MustCompile(
	`(?i)^((un)?licen[sc]e|copying)(\.md|\.txt|\.html|\.rst)?$`)

// isCanonicalLicenseFile reports whether the given filename matches a canonical
// license file pattern (LICENSE, COPYING, UNLICENSE, etc. with optional standard
// extensions), as opposed to variant files like LICENSE-MIT or LICENSE-3RD-PARTY.md.
func isCanonicalLicenseFile(name string) bool {
	return canonicalLicenseFileRe.MatchString(name)
}

type gitFiler struct {
	ctx          context.Context
	repo         *localrepo.Repo
	foundLicense bool
	path         string
	treeishID    git.ObjectID
}

func (f *gitFiler) ReadFile(path string) ([]byte, error) {
	data, err := f.repo.ReadObject(f.ctx, git.ObjectID(fmt.Sprintf("%s:%s", f.treeishID, path)))
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	// `licensedb.Detect` only opens files that look like licenses. Failing that, it will
	// also open readme files to try to identify license files. The RPC handler needs the
	// knowledge of whether any license files were encountered, so we filter out the
	// readme files as defined in licensedb.Detect:
	// https://github.com/go-enry/go-license-detector/blob/4f2ca6af2ab943d9b5fa3a02782eebc06f79a5f4/licensedb/internal/investigation.go#L61
	//
	// This doesn't filter out the possible license files identified from the readme files which may in fact not
	// be licenses.
	if !f.foundLicense {
		f.foundLicense = !readmeRegexp.MatchString(strings.ToLower(path))
		if f.foundLicense {
			f.path = path
		}
	}

	return data, nil
}

func (f *gitFiler) ReadDir(string) ([]filer.File, error) {
	var stderr bytes.Buffer
	cmd, err := f.repo.Exec(f.ctx, gitcmd.Command{
		Name: "ls-tree",
		Flags: []gitcmd.Option{
			gitcmd.Flag{Name: "-z"},
		},
		Args: []string{f.treeishID.String()},
	}, gitcmd.WithStderr(&stderr), gitcmd.WithSetupStdout())
	if err != nil {
		return nil, err
	}

	objectHash, err := f.repo.ObjectHash(f.ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	tree := localrepo.NewParser(cmd, objectHash)

	var files []filer.File
	for {
		entry, err := tree.NextEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if !entry.IsBlob() {
			continue
		}

		// Skip any file go-license-detector would use for NLP in
		// Plan B: take the README, find the section about the license and apply NER
		if readmeRegexp.MatchString(strings.ToLower(entry.Path)) {
			continue
		}

		files = append(files, filer.File{
			Name:  entry.Path,
			IsDir: false,
		})
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("ls-tree failed: %w, stderr: %q", err, stderr.String())
	}

	return files, nil
}

func (f *gitFiler) Close() {}

func (f *gitFiler) PathsAreAlwaysSlash() bool {
	// git ls-files uses unix slash `/`
	return true
}
