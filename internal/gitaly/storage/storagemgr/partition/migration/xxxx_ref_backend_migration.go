package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v16/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

// NewReferenceBackendMigration provides a new reference backend migration.
func NewReferenceBackendMigration(
	id uint64,
	targetBackend git.ReferenceBackend,
	localRepoFactory localrepo.Factory,
	flag *featureflag.FeatureFlag,
) Migration {
	name := fmt.Sprintf("migrate_ref_backend_%s", targetBackend.Name)

	return Migration{
		ID:   id,
		Name: name,
		Fn: func(ctx context.Context, tx storage.Transaction, storageName string, relativePath string) error {
			scopedFactory, err := localRepoFactory.ScopeByStorage(ctx, storageName)
			if err != nil {
				return fmt.Errorf("creating storage scoped factory: %w", err)
			}

			originalRepo := &gitalypb.Repository{RelativePath: relativePath}
			repo := scopedFactory.Build(tx.RewriteRepository(originalRepo).GetRelativePath())

			currentBackend, err := repo.ReferenceBackend(ctx)
			if err != nil {
				return fmt.Errorf("reference backend: %w", err)
			}

			// If the repository is already using the expected backend, there is
			// nothing to do.
			if currentBackend.Name == targetBackend.Name {
				return nil
			}

			getHash := func(repo *localrepo.Repo) ([]byte, error) {
				hasher := sha256.New()
				stderr := &bytes.Buffer{}

				err := repo.ExecAndWait(ctx, gitcmd.Command{
					Name: "for-each-ref",
					Flags: []gitcmd.Option{
						gitcmd.Flag{Name: "--format=%(refname) %(objectname) %(symref)"},
						gitcmd.Flag{Name: "--include-root-refs"},
					},
				}, gitcmd.WithStdout(hasher), gitcmd.WithStderr(stderr))
				if err != nil {
					return nil, fmt.Errorf("running for-each-ref: %w, stderr: %q", err, stderr)
				}

				return hasher.Sum(nil), nil
			}

			preMigrationHash, err := getHash(repo)
			if err != nil {
				return fmt.Errorf("hashing all refs pre-migration: %w", err)
			}

			repoPath, err := repo.Path(ctx)
			if err != nil {
				return fmt.Errorf("repo path: %w", err)
			}

			relPath, err := filepath.Rel(tx.FS().Root(), repoPath)
			if err != nil {
				return fmt.Errorf("relative path: %w", err)
			}

			if err := storage.RecordDirectoryRemoval(tx.FS(), tx.FS().Root(), filepath.Join(relPath, "refs")); err != nil {
				return fmt.Errorf("removing refs directory: %w", err)
			}

			switch targetBackend {
			case git.ReferenceBackendReftables:
				if err := tx.FS().RecordRemoval(filepath.Join(relPath, "packed-refs")); err != nil {
					return fmt.Errorf("removing packed-refs: %w", err)
				}
			case git.ReferenceBackendFiles:
				if err := storage.RecordDirectoryRemoval(tx.FS(), tx.FS().Root(), filepath.Join(relPath, "reftable")); err != nil {
					return fmt.Errorf("removing reftable directory: %w", err)
				}
			default:
				return fmt.Errorf("unknown backend provided: %s", targetBackend.Name)
			}

			if err := tx.FS().RecordRemoval(filepath.Join(relPath, "HEAD")); err != nil {
				return fmt.Errorf("removing HEAD: %w", err)
			}

			if err := tx.FS().RecordRemoval(filepath.Join(relPath, "config")); err != nil {
				return fmt.Errorf("removing config: %w", err)
			}

			stderr := &bytes.Buffer{}
			err = repo.ExecAndWait(ctx, gitcmd.Command{
				Name:   "refs",
				Action: "migrate",
				Flags: []gitcmd.Option{
					gitcmd.Flag{Name: fmt.Sprintf("--ref-format=%s", targetBackend.Name)},
					gitcmd.Flag{Name: "--no-reflog"},
				},
			}, gitcmd.WithStderr(stderr))
			if err != nil {
				return fmt.Errorf("running refs migrate: %w, stderr: %q", err, stderr)
			}

			// Since Git 2.48, the files backend will directly write to packed-refs
			// during ref-migrations. But for versions before that, we need to manually
			// pack the refs. Let's do that. We can remove this block once Git 2.48 is
			// made the minimum version.
			if targetBackend == git.ReferenceBackendFiles {
				gitVersion, err := repo.GitVersion(ctx)
				if err != nil {
					return fmt.Errorf("git version: %w", err)
				}

				if gitVersion.LessThan(git.NewVersion(2, 48, 0, 0)) {
					err = repo.ExecAndWait(ctx, gitcmd.Command{
						Name: "pack-refs",
						Flags: []gitcmd.Option{
							gitcmd.Flag{Name: "--all"},
						},
					}, gitcmd.WithStderr(stderr))
					if err != nil {
						return fmt.Errorf("running pack-refs: %w, stderr: %q", err, stderr)
					}
				}
			}

			postMigrationHash, err := getHash(repo)
			if err != nil {
				return fmt.Errorf("hashing all refs post-migration: %w", err)
			}

			if !bytes.Equal(preMigrationHash, postMigrationHash) {
				return errors.New("reference hash does not match")
			}

			if err := tx.FS().RecordFile(filepath.Join(relPath, "config")); err != nil {
				return fmt.Errorf("recording config: %w", err)
			}

			if err := tx.FS().RecordFile(filepath.Join(relPath, "HEAD")); err != nil {
				return fmt.Errorf("recording HEAD: %w", err)
			}

			switch targetBackend {
			case git.ReferenceBackendReftables:
				if err := tx.FS().RecordDirectory(filepath.Join(relPath, "refs")); err != nil {
					return fmt.Errorf("recording refs: %w", err)
				}

				if err := tx.FS().RecordFile(filepath.Join(relPath, "refs/heads")); err != nil {
					return fmt.Errorf("recording refs/heads: %w", err)
				}

				if err := storage.RecordDirectoryCreation(tx.FS(), filepath.Join(relPath, "reftable")); err != nil {
					return fmt.Errorf("recording reftable dir: %w", err)
				}
			case git.ReferenceBackendFiles:
				if err := tx.FS().RecordFile(filepath.Join(relPath, "packed-refs")); err != nil {
					return fmt.Errorf("recording packed-refs: %w", err)
				}

				if err := storage.RecordDirectoryCreation(tx.FS(), filepath.Join(relPath, "refs")); err != nil {
					return fmt.Errorf("recording refs dir: %w", err)
				}
			default:
				return fmt.Errorf("unknown backend provided: %s", targetBackend.Name)
			}

			return nil
		},
		IsDisabled: func(ctx context.Context) bool {
			if flag != nil {
				return flag.IsDisabled(ctx)
			}
			return false
		},
	}
}
