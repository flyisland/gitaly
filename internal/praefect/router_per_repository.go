package praefect

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

const (
	routeRepositoryAccessorPolicy            = "gitaly-route-repository-accessor-policy"
	routeRepositoryAccessorPolicyPrimaryOnly = "primary-only"
)

// errPrimaryUnassigned is returned when the primary node is not in the set of assigned nodes.
var errPrimaryUnassigned = errors.New("primary node is not assigned")

// AssignmentGetter is an interface for getting repository host node assignments.
type AssignmentGetter interface {
	// GetHostAssignments returns the names of the storages assigned to host the repository.
	// The primary node must always be assigned.
	GetHostAssignments(ctx context.Context, virtualStorage string, repositoryID int64) ([]string, error)
}

// ErrNoSuitableNode is returned when there is not suitable node to serve a request.
var ErrNoSuitableNode = errors.New("no suitable node to serve the request")

// ErrNoHealthyNodes is returned when there are no healthy nodes to serve a request.
var ErrNoHealthyNodes = errors.New("no healthy nodes")

// Connections is a set of connections to configured storage nodes by their virtual storages.
type Connections map[string]map[string]*grpc.ClientConn

// PrimaryGetter is an interface for getting a primary of a repository.
type PrimaryGetter interface {
	// GetPrimary returns the primary storage for a given repository.
	GetPrimary(ctx context.Context, virtualStorage string, repositoryID int64) (string, error)
}

// PerRepositoryRouter implements a router that routes requests respecting per repository primary nodes.
type PerRepositoryRouter struct {
	conns                     Connections
	ag                        AssignmentGetter
	pg                        PrimaryGetter
	rand                      Random
	hc                        HealthChecker
	csg                       datastore.ConsistentStoragesGetter
	rs                        datastore.RepositoryStore
	defaultReplicationFactors map[string]int
}

// NewPerRepositoryRouter returns a new PerRepositoryRouter using the passed configuration.
func NewPerRepositoryRouter(
	conns Connections,
	pg PrimaryGetter,
	hc HealthChecker,
	rand Random,
	csg datastore.ConsistentStoragesGetter,
	ag AssignmentGetter,
	rs datastore.RepositoryStore,
	defaultReplicationFactors map[string]int,
) *PerRepositoryRouter {
	return &PerRepositoryRouter{
		conns:                     conns,
		pg:                        pg,
		rand:                      rand,
		hc:                        hc,
		csg:                       csg,
		ag:                        ag,
		rs:                        rs,
		defaultReplicationFactors: defaultReplicationFactors,
	}
}

func (r *PerRepositoryRouter) healthyNodes(virtualStorage string) ([]RouterNode, error) {
	conns, ok := r.conns[virtualStorage]
	if !ok {
		return nil, nodes.ErrVirtualStorageNotExist
	}

	healthyNodes := make([]RouterNode, 0, len(conns))
	for _, storage := range r.hc.HealthyNodes()[virtualStorage] {
		conn, ok := conns[storage]
		if !ok {
			return nil, fmt.Errorf("no connection to node %q/%q", virtualStorage, storage)
		}

		healthyNodes = append(healthyNodes, RouterNode{
			Storage:    storage,
			Connection: conn,
		})
	}

	if len(healthyNodes) == 0 {
		return nil, ErrNoHealthyNodes
	}

	return healthyNodes, nil
}

func (r *PerRepositoryRouter) pickRandom(nodes []RouterNode) (RouterNode, error) {
	if len(nodes) == 0 {
		return RouterNode{}, ErrNoSuitableNode
	}

	return nodes[r.rand.Intn(len(nodes))], nil
}

// RouteStorageAccessor routes requests for storage-scoped accessor RPCs. The
// only storage scoped accessor RPC is RemoteService/FindRemoteRepository,
// which in turn executes a command without a repository. This can be done by
// any Gitaly server as it doesn't depend on the state on the server.
func (r *PerRepositoryRouter) RouteStorageAccessor(ctx context.Context, virtualStorage string) (RouterNode, error) {
	healthyNodes, err := r.healthyNodes(virtualStorage)
	if err != nil {
		return RouterNode{}, err
	}

	return r.pickRandom(healthyNodes)
}

// RouteStorageMutator is not implemented here. The only storage scoped mutator RPC is related to namespace operations.
// These are not relevant anymore, given hashed storage is default everywhere, and should be eventually removed.
func (r *PerRepositoryRouter) RouteStorageMutator(ctx context.Context, virtualStorage string) (StorageMutatorRoute, error) {
	return StorageMutatorRoute{}, errors.New("RouteStorageMutator is not implemented on PerRepositoryRouter")
}

//nolint:revive // This is unintentionally missing documentation.
func (r *PerRepositoryRouter) RouteRepositoryAccessor(ctx context.Context, virtualStorage, relativePath string, forcePrimary bool) (RepositoryAccessorRoute, error) {
	healthyNodes, err := r.healthyNodes(virtualStorage)
	if err != nil {
		return RepositoryAccessorRoute{}, err
	}

	if forcePrimary {
		repositoryID, err := r.rs.GetRepositoryID(ctx, virtualStorage, relativePath)
		if err != nil {
			return RepositoryAccessorRoute{}, fmt.Errorf("get repository id: %w", err)
		}

		primary, err := r.pg.GetPrimary(ctx, virtualStorage, repositoryID)
		if err != nil {
			return RepositoryAccessorRoute{}, fmt.Errorf("get primary: %w", err)
		}

		replicaPath, _, err := r.rs.GetConsistentStoragesByRepositoryID(ctx, repositoryID)
		if err != nil {
			return RepositoryAccessorRoute{}, fmt.Errorf("get replica path: %w", err)
		}

		for _, node := range healthyNodes {
			if node.Storage == primary {
				return RepositoryAccessorRoute{
					ReplicaPath: replicaPath,
					Node:        node,
				}, nil
			}
		}

		return RepositoryAccessorRoute{}, nodes.ErrPrimaryNotHealthy
	}

	replicaPath, consistentStorages, err := r.csg.GetConsistentStorages(ctx, virtualStorage, relativePath)
	if err != nil {
		return RepositoryAccessorRoute{}, fmt.Errorf("consistent storages: %w", err)
	}

	healthyConsistentNodes := make([]RouterNode, 0, len(healthyNodes))
	for _, node := range healthyNodes {
		if !consistentStorages.HasValue(node.Storage) {
			continue
		}

		healthyConsistentNodes = append(healthyConsistentNodes, node)
	}

	node, err := r.pickRandom(healthyConsistentNodes)
	if err != nil {
		return RepositoryAccessorRoute{}, err
	}

	return RepositoryAccessorRoute{
		ReplicaPath: replicaPath,
		Node:        node,
	}, nil
}

func (r *PerRepositoryRouter) resolveAdditionalReplicaPath(ctx context.Context, virtualStorage, additionalRelativePath string) (string, error) {
	if additionalRelativePath == "" {
		return "", nil
	}

	additionalRepositoryID, err := r.rs.GetRepositoryID(ctx, virtualStorage, additionalRelativePath)
	if err != nil {
		if errors.Is(err, datastore.ErrRepositoryNotFound) {
			return "", additionalRepositoryNotFoundError{
				storageName:  virtualStorage,
				relativePath: additionalRelativePath,
			}
		}

		return "", fmt.Errorf("get additional repository id: %w", err)
	}

	replicaPath, err := r.rs.GetReplicaPath(ctx, additionalRepositoryID)
	if err != nil {
		return "", err
	}

	return replicaPath, nil
}

//nolint:revive // This is unintentionally missing documentation.
func (r *PerRepositoryRouter) RouteRepositoryMutator(ctx context.Context, virtualStorage, relativePath, additionalRelativePath string) (RepositoryMutatorRoute, error) {
	healthyNodes, err := r.healthyNodes(virtualStorage)
	if err != nil {
		return RepositoryMutatorRoute{}, err
	}

	repositoryID, err := r.rs.GetRepositoryID(ctx, virtualStorage, relativePath)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get repository id: %w", err)
	}

	additionalReplicaPath, err := r.resolveAdditionalReplicaPath(ctx, virtualStorage, additionalRelativePath)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("resolve additional replica path: %w", err)
	}

	primary, err := r.pg.GetPrimary(ctx, virtualStorage, repositoryID)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get primary: %w", err)
	}

	healthySet := make(map[string]RouterNode)
	for _, node := range healthyNodes {
		healthySet[node.Storage] = node
	}

	if _, ok := healthySet[primary]; !ok {
		return RepositoryMutatorRoute{}, nodes.ErrPrimaryNotHealthy
	}

	replicaPath, consistentStorages, err := r.rs.GetConsistentStoragesByRepositoryID(ctx, repositoryID)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("consistent storages: %w", err)
	}

	if !consistentStorages.HasValue(primary) {
		return RepositoryMutatorRoute{}, ErrRepositoryReadOnly
	}

	assignedStorages, err := r.ag.GetHostAssignments(ctx, virtualStorage, repositoryID)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("get host assignments: %w", err)
	}

	route := RepositoryMutatorRoute{
		RepositoryID:          repositoryID,
		ReplicaPath:           replicaPath,
		AdditionalReplicaPath: additionalReplicaPath,
	}
	for _, assigned := range assignedStorages {
		node, healthy := healthySet[assigned]
		if assigned == primary {
			route.Primary = node
			continue
		}

		if !consistentStorages.HasValue(node.Storage) || !healthy {
			route.ReplicationTargets = append(route.ReplicationTargets, assigned)
			continue
		}

		route.Secondaries = append(route.Secondaries, node)
	}

	if (route.Primary == RouterNode{}) {
		// AssignmentGetter interface defines that the primary must always be assigned.
		// While this case should not commonly happen, we must handle it here for now.
		// This can be triggered if the primary is demoted and unassigned during the RPC call.
		// The three SQL queries above are done non-transactionally. Once the variable
		// replication factor and repository specific primaries are enabled by default, we should
		// combine the above queries in to a single call to remove this case and make the
		// whole operation more efficient.
		return RepositoryMutatorRoute{}, errPrimaryUnassigned
	}

	return route, nil
}

type assignedNodes struct {
	primary            RouterNode
	secondaries        []RouterNode
	replicationTargets []string
}

// assignRepositoryToNodes picks a random healthy node to act as the primary node and selects the
// secondary nodes if assignments are enabled. Healthy secondaries take part in the transaction,
// unhealthy secondaries are set as replication targets.
func (r *PerRepositoryRouter) assignRepositoryToNodes(
	virtualStorage string,
	additionalRepoMetadata *datastore.RepositoryMetadata,
) (assignedNodes, error) {
	healthyNodes, err := r.healthyNodes(virtualStorage)
	if err != nil {
		return assignedNodes{}, err
	}

	replicationFactor := r.defaultReplicationFactors[virtualStorage]

	switch {
	case additionalRepoMetadata != nil:
		// RPCs that create repositories can have an additional repository. This repository
		// is used as additional input and is expected to be directly accessible on the
		// target node, or otherwise we wouldn't have to resolve its relative path.
		//
		// It follows that the newly created repository and the additional repository must
		// live on the same storage nodes. So instead of picking primary and secondaries
		// randomly, we instead choose the same storage nodes as the preexisting additional
		// repository is assigned to.

		healthyNodesByStorage := map[string]RouterNode{}
		for _, healthyNode := range healthyNodes {
			healthyNodesByStorage[healthyNode.Storage] = healthyNode
		}

		primary, ok := healthyNodesByStorage[additionalRepoMetadata.Primary]
		if !ok {
			return assignedNodes{}, nodes.ErrPrimaryNotHealthy
		}

		// For every secondary we need to figure out whether it's in a state so that we can
		// create it on them directly or whether we need to replicate asynchronously
		// instead.
		var healthySecondaries []RouterNode
		var replicationTargets []string
		for _, replica := range additionalRepoMetadata.Replicas {
			if replica.Storage == primary.Storage {
				continue
			}

			// An unhealthy storage cannot serve requests, so we set these up as a
			// replication target.
			node, ok := healthyNodesByStorage[replica.Storage]
			if !ok {
				replicationTargets = append(replicationTargets, replica.Storage)
				continue
			}

			// We expect that the additional repository will be used by the RPC call, so
			// if its state differs from the primary node it is likely that the end
			// result would differ, as well. So in case the generation numbers are not
			// equal we'll thus set it up as a replication target instead and rely on
			// ReplicateRepository to fix things up for us.
			if replica.Generation != additionalRepoMetadata.Generation {
				replicationTargets = append(replicationTargets, replica.Storage)
				continue
			}

			healthySecondaries = append(healthySecondaries, node)
		}

		return assignedNodes{
			primary:            primary,
			secondaries:        healthySecondaries,
			replicationTargets: replicationTargets,
		}, nil
	case replicationFactor == 1:
		// If we have a replication factor of 1 then picking the primary node is already
		// sufficient.
		primary, err := r.pickRandom(healthyNodes)
		if err != nil {
			return assignedNodes{}, err
		}

		return assignedNodes{
			primary: primary,
		}, nil
	default:
		// Otherwise, we need to figure out what the actual replication factor is supposed
		// to be and assign secondaries as required to satisfy it.

		primary, err := r.pickRandom(healthyNodes)
		if err != nil {
			return assignedNodes{}, err
		}

		var secondaryNodes []RouterNode
		for storage, conn := range r.conns[virtualStorage] {
			if storage == primary.Storage {
				continue
			}

			secondaryNodes = append(secondaryNodes, RouterNode{
				Storage:    storage,
				Connection: conn,
			})
		}

		// replicationFactor being zero indicates it has not been configured. If so, we
		// fallback to the behavior of no assignments and replicate everywhere. Otherwise,
		// if we have a positive replication factor, we pick a random set of secondaries.
		if replicationFactor > 0 {
			// Select random secondaries according to the default replication factor.
			r.rand.Shuffle(len(secondaryNodes), func(i, j int) {
				secondaryNodes[i], secondaryNodes[j] = secondaryNodes[j], secondaryNodes[i]
			})

			secondaryNodes = secondaryNodes[:replicationFactor-1]
		}

		// We now split up secondaries into two sets: those which are known to be healthy
		// and those which aren't. This allows us to decide whether we'll route to the
		// secondary node immediately or instead create a replication job.
		var healthySecondaries []RouterNode
		var replicationTargets []string
		for _, secondaryNode := range secondaryNodes {
			isHealthy := false
			for _, healthyNode := range healthyNodes {
				if healthyNode == secondaryNode {
					isHealthy = true
					break
				}
			}

			if isHealthy {
				healthySecondaries = append(healthySecondaries, secondaryNode)
				continue
			}

			replicationTargets = append(replicationTargets, secondaryNode.Storage)
		}

		return assignedNodes{
			primary:            primary,
			secondaries:        healthySecondaries,
			replicationTargets: replicationTargets,
		}, nil
	}
}

// RouteRepositoryCreation routes an incoming repository creation to a set of target nodes that will
// be designated to hold the new repository.
func (r *PerRepositoryRouter) RouteRepositoryCreation(ctx context.Context, virtualStorage, relativePath, additionalRelativePath string) (RepositoryMutatorRoute, error) {
	var additionalRepoMetadata *datastore.RepositoryMetadata
	var additionalReplicaPath string
	if additionalRelativePath != "" {
		metadata, err := r.rs.GetRepositoryMetadataByPath(ctx, virtualStorage, additionalRelativePath)
		if err != nil {
			if errors.Is(err, datastore.ErrRepositoryNotFound) {
				return RepositoryMutatorRoute{}, additionalRepositoryNotFoundError{
					storageName:  virtualStorage,
					relativePath: additionalRelativePath,
				}
			}

			return RepositoryMutatorRoute{}, fmt.Errorf("resolve additional replica metadata: %w", err)
		}

		additionalRepoMetadata = &metadata
		additionalReplicaPath = metadata.ReplicaPath
	}

	assignedNodes, err := r.assignRepositoryToNodes(virtualStorage, additionalRepoMetadata)
	if err != nil {
		return RepositoryMutatorRoute{}, err
	}

	id, err := r.rs.ReserveRepositoryID(ctx, virtualStorage, relativePath)
	if err != nil {
		return RepositoryMutatorRoute{}, fmt.Errorf("reserve repository id: %w", err)
	}

	replicaPath := storage.DeriveReplicaPath(id)
	if storage.IsRailsPoolRepository(&gitalypb.Repository{
		StorageName:  virtualStorage,
		RelativePath: relativePath,
	}) {
		replicaPath = storage.DerivePoolPath(id)
	}

	return RepositoryMutatorRoute{
		RepositoryID:          id,
		ReplicaPath:           replicaPath,
		AdditionalReplicaPath: additionalReplicaPath,
		Primary:               assignedNodes.primary,
		Secondaries:           assignedNodes.secondaries,
		ReplicationTargets:    assignedNodes.replicationTargets,
	}, nil
}

// RouteRepositoryMaintenance will route the maintenance call to all healthy nodes in a best-effort
// strategy. We do not raise an error in case the primary node is unhealthy, but will in case all
// nodes are unhealthy.
func (r *PerRepositoryRouter) RouteRepositoryMaintenance(ctx context.Context, virtualStorage, relativePath string) (RepositoryMaintenanceRoute, error) {
	healthyNodes, err := r.healthyNodes(virtualStorage)
	if err != nil {
		return RepositoryMaintenanceRoute{}, err
	}

	healthyNodesByStorage := map[string]RouterNode{}
	for _, healthyNode := range healthyNodes {
		healthyNodesByStorage[healthyNode.Storage] = healthyNode
	}

	metadata, err := r.rs.GetRepositoryMetadataByPath(ctx, virtualStorage, relativePath)
	if err != nil {
		return RepositoryMaintenanceRoute{}, fmt.Errorf("getting repository metadata: %w", err)
	}

	nodes := make([]RouterNode, 0, len(metadata.Replicas))
	for _, replica := range metadata.Replicas {
		node, ok := healthyNodesByStorage[replica.Storage]
		if !ok {
			continue
		}

		// If the is not assigned to the replica it either hasn't yet been created
		// or it will eventually get deleted. In neither case does it make sense to
		// maintain it, so we skip such nodes.
		if !replica.Assigned {
			continue
		}

		// If the repository doesn't exist on the replica there is no need to perform any
		// maintenance tasks at all.
		if replica.Generation < 0 {
			continue
		}

		nodes = append(nodes, node)
	}

	if len(nodes) == 0 {
		return RepositoryMaintenanceRoute{}, ErrNoHealthyNodes
	}

	return RepositoryMaintenanceRoute{
		RepositoryID: metadata.RepositoryID,
		ReplicaPath:  metadata.ReplicaPath,
		Nodes:        nodes,
	}, nil
}
