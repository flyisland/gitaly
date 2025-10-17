package praefect

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v18/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/transactions"
)

// Dependencies consolidates Praefect service dependencies for injection.
type Dependencies struct {
	Config          config.Config
	Logger          log.Logger
	Coordinator     *Coordinator
	Director        proxy.StreamDirector
	NodeMgr         nodes.Manager
	TxMgr           *transactions.Manager
	Queue           datastore.ReplicationEventQueue
	RepositoryStore datastore.RepositoryStore
	AssignmentStore AssignmentStore
	Router          Router
	Registry        *protoregistry.Registry
	Conns           Connections
	PrimaryGetter   PrimaryGetter
	Checks          []service.CheckFunc
}
