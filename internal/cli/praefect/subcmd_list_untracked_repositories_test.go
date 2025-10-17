package praefect

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

func TestListUntrackedRepositoriesCommand(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Addr := testserver.RunGitalyServer(t, g2Cfg, setup.RegisterAll, testserver.WithDisablePraefect())

	db := testdb.New(t)
	var database string
	require.NoError(t, db.QueryRow(`SELECT current_database()`).Scan(&database))
	dbConf := testdb.GetConfig(t, database)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Addr},
				},
			},
		},
		DB: dbConf,
	}

	confPath := writeConfigToFile(t, conf)

	praefectServer := testserver.StartPraefect(t, conf)

	cc, err := client.New(ctx, praefectServer.Address())
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.Close()) }()
	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	praefectStorage := conf.VirtualStorages[0].Name

	// Repository managed by praefect, exists on gitaly-1 and gitaly-2.
	createRepo(t, ctx, repoClient, praefectStorage, "path/to/test/repo")

	// Repositories not managed by praefect.
	repo1, repo1Path := gittest.CreateRepository(t, ctx, g1Cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	repo2, repo2Path := gittest.CreateRepository(t, ctx, g1Cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})
	_, _ = gittest.CreateRepository(t, ctx, g2Cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	timeDelta := 4 * time.Hour
	require.NoError(t, os.Chtimes(
		repo1Path,
		time.Now().Add(-(timeDelta+1*time.Second)),
		time.Now().Add(-(timeDelta+1*time.Second))))
	require.NoError(t, os.Chtimes(
		repo2Path,
		time.Now().Add(-(timeDelta+1*time.Second)),
		time.Now().Add(-(timeDelta+1*time.Second))))

	t.Run("positional arguments", func(t *testing.T) {
		stdout, stderr, exitCode := runApp(t, ctx, []string{"-config", confPath, "list-untracked-repositories", "positional-arg"})
		assert.NotEmpty(t, stdout, "the help text should be printed")
		assert.Equal(t, cli.Exit(unexpectedPositionalArgsError{Command: "list-untracked-repositories"}, 1).Error()+"\n", stderr)
		require.Equal(t, 1, exitCode)
	})

	t.Run("default flag values used", func(t *testing.T) {
		stdout, stderr, exitCode := runApp(t, ctx, []string{"-config", confPath, "list-untracked-repositories"})
		require.Zero(t, exitCode)
		assert.Empty(t, stdout)
		assert.Empty(t, stderr)
	})

	t.Run("passed flag values used", func(t *testing.T) {
		stdout, stderr, exitCode := runApp(t, ctx, []string{"-config", confPath, "list-untracked-repositories", "-older-than", timeDelta.String(), "-delimiter", "~"})
		require.Zero(t, exitCode)
		assert.Empty(t, stderr)
		exp := []string{
			"The following repositories were found on disk, but missing from the tracking database:",
			fmt.Sprintf(`{"relative_path":%q,"storage":"gitaly-1","virtual_storage":"praefect"}`, repo1.GetRelativePath()),
			fmt.Sprintf(`{"relative_path":%q,"storage":"gitaly-1","virtual_storage":"praefect"}`, repo2.GetRelativePath()),
			"", // an empty extra element required as each line ends with "delimiter" and strings.Split returns all parts
		}
		elems := strings.Split(stdout, "~")
		require.Len(t, elems, len(exp)-1)
		elems = append(elems[1:], strings.Split(elems[0], "\n")...)
		require.ElementsMatch(t, exp, elems)
	})
}

func createRepo(t *testing.T, ctx context.Context, repoClient gitalypb.RepositoryServiceClient, storageName, relativePath string) *gitalypb.Repository {
	t.Helper()
	repo := &gitalypb.Repository{
		StorageName:  storageName,
		RelativePath: relativePath,
	}

	_, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})
	require.NoError(t, err)

	return repo
}
