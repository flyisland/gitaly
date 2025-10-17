package storagemgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/keyvalue/databasemgr"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	logging "gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func getTestDBManager(t *testing.T, ctx context.Context, cfg config.Cfg, logger logging.Logger) (*databasemgr.DBManager, keyvalue.Store) {
	t.Helper()

	dbMgr, err := databasemgr.NewDBManager(ctx, cfg.Storages, keyvalue.NewBadgerStore, helper.NewNullTickerFactory(), logger)
	require.NoError(t, err)
	t.Cleanup(dbMgr.Close)

	db, err := dbMgr.GetDB(cfg.Storages[0].Name)
	require.NoError(t, err)

	return dbMgr, db
}
