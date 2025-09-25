package praefect

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// StaticRepositoryAssignments is a static assignment of storages for each individual repository.
type StaticRepositoryAssignments map[string]map[string][]string

func (st StaticRepositoryAssignments) GetHostAssignments(ctx context.Context, virtualStorage, relativePath string) ([]string, error) {
	vs, ok := st[virtualStorage]
	if !ok {
		return nil, nodes.ErrVirtualStorageNotExist
	}

	storages, ok := vs[relativePath]
	if !ok {
		return nil, datastore.ErrRepositoryNotFound
	}

	return storages, nil
}

func TestPerRepositoryRouter_RouteStorageAccessor(t *testing.T) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc           string
		virtualStorage string
		numCandidates  int
		pickCandidate  int
		error          error
		node           string
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:           "picks randomly first candidate",
			virtualStorage: "virtual-storage-1",
			numCandidates:  2,
			pickCandidate:  0,
			node:           "valid-choice-1",
		},
		{
			desc:           "picks randomly second candidate",
			virtualStorage: "virtual-storage-1",
			numCandidates:  2,
			pickCandidate:  1,
			node:           "valid-choice-2",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			conns := Connections{
				"virtual-storage-1": {
					"valid-choice-1": &grpc.ClientConn{},
					"valid-choice-2": &grpc.ClientConn{},
					"unhealthy":      &grpc.ClientConn{},
				},
			}

			router := NewPerRepositoryRouter(
				conns,
				nil,
				StaticHealthChecker{
					"virtual-storage-1": {
						"valid-choice-1",
						"valid-choice-2",
					},
				},
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.numCandidates, n)
						return tc.pickCandidate
					},
				},
				nil,
				nil,
				datastore.MockRepositoryStore{},
				nil,
			)

			node, err := router.RouteStorageAccessor(ctx, tc.virtualStorage)
			require.Equal(t, tc.error, err)
			require.Equal(t, RouterNode{
				Storage:    tc.node,
				Connection: conns["virtual-storage-1"][tc.node],
			}, node)
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryAccessor(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)

	const relativePath = "repository"

	for _, tc := range []struct {
		desc           string
		virtualStorage string
		healthyNodes   StaticHealthChecker
		metadata       map[string]string
		forcePrimary   bool
		numCandidates  int
		pickCandidate  int
		error          error
		node           string
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:           "no healthy nodes",
			virtualStorage: "virtual-storage-1",
			healthyNodes:   map[string][]string{},
			error:          ErrNoHealthyNodes,
		},
		{
			desc:           "primary picked randomly",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			numCandidates: 2,
			pickCandidate: 0,
			node:          "primary",
		},
		{
			desc:           "secondary picked randomly",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			numCandidates: 2,
			pickCandidate: 1,
			node:          "consistent-secondary",
		},
		{
			desc:           "secondary picked when primary is unhealthy",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"consistent-secondary"},
			},
			numCandidates: 1,
			node:          "consistent-secondary",
		},
		{
			desc:           "no suitable nodes",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"inconsistent-secondary"},
			},
			error: ErrNoSuitableNode,
		},
		{
			desc:           "primary force-picked",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"primary", "consistent-secondary"},
			},
			forcePrimary: true,
			node:         "primary",
		},
		{
			desc:           "secondary not picked if force-picking unhealthy primary",
			virtualStorage: "virtual-storage-1",
			healthyNodes: map[string][]string{
				"virtual-storage-1": {"consistent-secondary"},
			},
			forcePrimary: true,
			error:        nodes.ErrPrimaryNotHealthy,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			ctx = testhelper.MergeIncomingMetadata(ctx, metadata.New(tc.metadata))

			conns := Connections{
				"virtual-storage-1": {
					"primary":                &grpc.ClientConn{},
					"consistent-secondary":   &grpc.ClientConn{},
					"inconsistent-secondary": &grpc.ClientConn{},
					"unhealthy-secondary":    &grpc.ClientConn{},
				},
			}

			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": {
				"virtual-storage-1": {"primary", "consistent-secondary", "inconsistent-secondary"},
			}})

			rs := datastore.NewPostgresRepositoryStore(tx, nil)
			repositoryID, err := rs.ReserveRepositoryID(ctx, "virtual-storage-1", relativePath)
			require.NoError(t, err)
			require.NoError(t,
				rs.CreateRepository(ctx, repositoryID, "virtual-storage-1", relativePath, relativePath, "primary",
					[]string{"consistent-secondary", "unhealthy-secondary", "inconsistent-secondary"}, nil, true, true),
			)
			require.NoError(t,
				rs.IncrementGeneration(ctx, repositoryID, "primary", []string{"consistent-secondary", "unhealthy-secondary"}),
			)

			router := NewPerRepositoryRouter(
				conns,
				nodes.NewPerRepositoryElector(testhelper.NewLogger(t), tx),
				tc.healthyNodes,
				mockRandom{
					intnFunc: func(n int) int {
						require.Equal(t, tc.numCandidates, n)
						return tc.pickCandidate
					},
				},
				rs,
				nil,
				rs,
				nil,
			)

			route, err := router.RouteRepositoryAccessor(ctx, tc.virtualStorage, relativePath, tc.forcePrimary)
			require.Equal(t, tc.error, err)
			if tc.node != "" {
				require.Equal(t,
					RepositoryAccessorRoute{
						ReplicaPath: relativePath,
						Node: RouterNode{
							Storage:    tc.node,
							Connection: conns[tc.virtualStorage][tc.node],
						},
					},
					route)
			} else {
				require.Empty(t, route)
			}
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryMutator(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)

	configuredNodes := map[string][]string{
		"virtual-storage-1": {"primary", "secondary-1", "secondary-2"},
	}

	for _, tc := range []struct {
		desc                   string
		virtualStorage         string
		healthyNodes           StaticHealthChecker
		consistentStorages     []string
		secondaries            []string
		replicationTargets     []string
		error                  error
		assignedNodes          StaticRepositoryAssignments
		noAdditionalRepository bool
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			error:          nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:               "outdated primary is demoted",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-2"}},
			consistentStorages: []string{"secondary-1"},
			error:              nodes.ErrPrimaryNotHealthy,
		},
		{
			desc:               "primary unhealthy",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"secondary-1", "secondary-2"}},
			consistentStorages: []string{"primary"},
			error:              nodes.ErrPrimaryNotHealthy,
		},
		{
			desc:               "all secondaries consistent",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1", "secondary-2"},
		},
		{
			desc:                   "no additional repository",
			virtualStorage:         "virtual-storage-1",
			healthyNodes:           StaticHealthChecker(configuredNodes),
			consistentStorages:     []string{"primary", "secondary-1", "secondary-2"},
			secondaries:            []string{"secondary-1", "secondary-2"},
			noAdditionalRepository: true,
		},
		{
			desc:               "inconsistent secondary",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			consistentStorages: []string{"primary", "secondary-2"},
			secondaries:        []string{"secondary-2"},
			replicationTargets: []string{"secondary-1"},
		},
		{
			desc:               "unhealthy secondaries",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker{"virtual-storage-1": {"primary"}},
			consistentStorages: []string{"primary", "secondary-1"},
			replicationTargets: []string{"secondary-1", "secondary-2"},
		},
		{
			desc:               "up to date unassigned nodes are ignored",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"primary", "secondary-1"}}},
			consistentStorages: []string{"primary", "secondary-1", "secondary-2"},
			secondaries:        []string{"secondary-1"},
		},
		{
			desc:               "outdated unassigned nodes are ignored",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"primary", "secondary-1"}}},
			consistentStorages: []string{"primary", "secondary-1"},
			secondaries:        []string{"secondary-1"},
		},
		{
			desc:               "primary is unassigned",
			virtualStorage:     "virtual-storage-1",
			healthyNodes:       StaticHealthChecker(configuredNodes),
			assignedNodes:      StaticRepositoryAssignments{"virtual-storage-1": {"repository": {"secondary-1", "secondary-2"}}},
			consistentStorages: []string{"primary"},
			error:              errPrimaryUnassigned,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			conns := Connections{
				"virtual-storage-1": {
					"primary":     &grpc.ClientConn{},
					"secondary-1": &grpc.ClientConn{},
					"secondary-2": &grpc.ClientConn{},
				},
			}

			tx := db.Begin(t)
			defer tx.Rollback(t)

			testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": configuredNodes})

			const (
				virtualStorage         = "virtual-storage-1"
				relativePath           = "repository"
				additionalRelativePath = "additional-repository"
				additionalReplicaPath  = "additional-replica-path"
			)

			rs := datastore.NewPostgresRepositoryStore(tx, nil)
			repositoryID, err := rs.ReserveRepositoryID(ctx, virtualStorage, relativePath)
			require.NoError(t, err)

			require.NoError(t,
				rs.CreateRepository(ctx, repositoryID, virtualStorage, relativePath, relativePath, "primary", []string{"secondary-1", "secondary-2"}, nil, true, false),
			)

			additionalRepositoryID, err := rs.ReserveRepositoryID(ctx, virtualStorage, additionalRelativePath)
			require.NoError(t, err)
			require.NoError(t,
				rs.CreateRepository(ctx, additionalRepositoryID, virtualStorage, additionalRelativePath, additionalReplicaPath, "primary", nil, nil, true, false),
			)

			if len(tc.consistentStorages) > 0 {
				require.NoError(t, rs.IncrementGeneration(ctx, repositoryID, tc.consistentStorages[0], tc.consistentStorages[1:]))
			}

			for virtualStorage, relativePaths := range tc.assignedNodes {
				for relativePath, storages := range relativePaths {
					for _, storage := range storages {
						_, err := tx.ExecContext(ctx, `
							INSERT INTO repository_assignments
							VALUES ($1, $2, $3, $4)
						`, virtualStorage, relativePath, storage, repositoryID)
						require.NoError(t, err)
					}
				}
			}

			router := NewPerRepositoryRouter(
				conns,
				nodes.NewPerRepositoryElector(testhelper.NewLogger(t), tx),
				tc.healthyNodes,
				nil,
				rs,
				datastore.NewAssignmentStore(tx, configuredNodes),
				rs,
				nil,
			)

			requestAdditionalRelativePath := additionalRelativePath
			expectedAdditionalReplicaPath := additionalReplicaPath
			if tc.noAdditionalRepository {
				expectedAdditionalReplicaPath = ""
				requestAdditionalRelativePath = ""
			}

			route, err := router.RouteRepositoryMutator(ctx, tc.virtualStorage, relativePath, requestAdditionalRelativePath)
			require.Equal(t, tc.error, err)
			if err == nil {
				var secondaries []RouterNode
				for _, secondary := range tc.secondaries {
					secondaries = append(secondaries, RouterNode{
						Storage:    secondary,
						Connection: conns[tc.virtualStorage][secondary],
					})
				}

				require.Equal(t, RepositoryMutatorRoute{
					RepositoryID:          repositoryID,
					ReplicaPath:           relativePath,
					AdditionalReplicaPath: expectedAdditionalReplicaPath,
					Primary: RouterNode{
						Storage:    "primary",
						Connection: conns[tc.virtualStorage]["primary"],
					},
					Secondaries:        secondaries,
					ReplicationTargets: tc.replicationTargets,
				}, route)
			}
		})
	}
}

func TestPerRepositoryRouter_RouteRepositoryMaintenance(t *testing.T) {
	t.Parallel()

	db := testdb.New(t)

	var (
		virtualStorage = "virtual-storage-1"
		relativePath   = gittest.NewRepositoryName(t)
	)

	configuredStorages := []string{"primary", "secondary-1", "secondary-2"}

	for _, tc := range []struct {
		desc               string
		virtualStorage     string
		healthyStorages    []string
		assignedStorages   []string
		storageGenerations map[string]int
		expectedStorages   []string
		expectedErr        error
	}{
		{
			desc:           "unknown virtual storage",
			virtualStorage: "unknown",
			expectedErr:    nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:           "all nodes unhealthy",
			virtualStorage: virtualStorage,
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedErr: ErrNoHealthyNodes,
		},
		{
			desc:            "unhealthy primary",
			virtualStorage:  virtualStorage,
			healthyStorages: []string{"secondary-1", "secondary-2"},
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"secondary-1", "secondary-2"},
		},
		{
			desc:            "unhealthy secondaries",
			virtualStorage:  virtualStorage,
			healthyStorages: []string{"primary"},
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"primary"},
		},
		{
			desc:            "all nodes healthy",
			virtualStorage:  virtualStorage,
			healthyStorages: configuredStorages,
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"primary", "secondary-1", "secondary-2"},
		},
		{
			desc:             "unassigned primary is ignored",
			virtualStorage:   virtualStorage,
			healthyStorages:  configuredStorages,
			assignedStorages: []string{"secondary-1", "secondary-2"},
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"secondary-1", "secondary-2"},
		},
		{
			desc:             "unassigned secondary is ignored",
			virtualStorage:   virtualStorage,
			healthyStorages:  configuredStorages,
			assignedStorages: []string{"primary", "secondary-1"},
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"primary", "secondary-1"},
		},
		{
			desc:            "no assigned nodes",
			virtualStorage:  virtualStorage,
			healthyStorages: configuredStorages,
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"primary", "secondary-1", "secondary-2"},
		},
		{
			desc:             "unhealthy and unassigned nodes",
			virtualStorage:   virtualStorage,
			healthyStorages:  []string{"primary", "secondary-1"},
			assignedStorages: []string{"primary", "secondary-2"},
			storageGenerations: map[string]int{
				"primary":     1,
				"secondary-1": 1,
				"secondary-2": 1,
			},
			expectedStorages: []string{"primary"},
		},
		{
			desc:             "missing repo on primary",
			virtualStorage:   virtualStorage,
			healthyStorages:  configuredStorages,
			assignedStorages: configuredStorages,
			storageGenerations: map[string]int{
				"primary":     -1,
				"secondary-1": 9000,
				"secondary-2": 9000,
			},
			expectedStorages: []string{"secondary-1", "secondary-2"},
		},
		{
			desc:             "missing repo on secondary",
			virtualStorage:   virtualStorage,
			healthyStorages:  configuredStorages,
			assignedStorages: configuredStorages,
			storageGenerations: map[string]int{
				"primary":     9000,
				"secondary-1": 9000,
				"secondary-2": -1,
			},
			expectedStorages: []string{"primary", "secondary-1"},
		},
		{
			desc:             "mixed generations",
			virtualStorage:   virtualStorage,
			healthyStorages:  configuredStorages,
			assignedStorages: configuredStorages,
			storageGenerations: map[string]int{
				"primary":     0,
				"secondary-1": 1,
				"secondary-2": 2,
			},
			expectedStorages: []string{"primary", "secondary-1", "secondary-2"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			tx := db.Begin(t)
			defer tx.Rollback(t)

			conns := Connections{
				virtualStorage: {
					"primary":     &grpc.ClientConn{},
					"secondary-1": &grpc.ClientConn{},
					"secondary-2": &grpc.ClientConn{},
				},
			}

			rs := datastore.NewPostgresRepositoryStore(tx, map[string][]string{
				virtualStorage: configuredStorages,
			})

			repositoryID, err := rs.ReserveRepositoryID(ctx, virtualStorage, relativePath)
			require.NoError(t, err)
			require.NoError(t, rs.CreateRepository(ctx, repositoryID, virtualStorage, relativePath, relativePath, "primary", []string{"secondary-1", "secondary-2"}, nil, true, false))

			for _, storage := range tc.assignedStorages {
				_, err := tx.ExecContext(ctx, `
				INSERT INTO repository_assignments
				VALUES ($1, $2, $3, $4)
				`, virtualStorage, relativePath, storage, repositoryID)
				require.NoError(t, err)
			}

			for storage, generation := range tc.storageGenerations {
				require.NoError(t, rs.SetGeneration(ctx, repositoryID, storage, relativePath, generation))
			}

			router := NewPerRepositoryRouter(conns, nil, StaticHealthChecker{
				virtualStorage: tc.healthyStorages,
			}, nil, nil, nil, rs, nil)

			route, err := router.RouteRepositoryMaintenance(ctx, tc.virtualStorage, relativePath)
			require.Equal(t, tc.expectedErr, err)
			if err == nil {
				var expectedStorages []RouterNode
				for _, expectedNode := range tc.expectedStorages {
					expectedStorages = append(expectedStorages, RouterNode{
						Storage:    expectedNode,
						Connection: conns[tc.virtualStorage][expectedNode],
					})
				}

				require.Equal(t, RepositoryMaintenanceRoute{
					RepositoryID: repositoryID,
					ReplicaPath:  relativePath,
					Nodes:        expectedStorages,
				}, route)
			}
		})
	}
}

func TestPerRepositoryRouterRouteRepositoryCreation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	configuredNodes := map[string][]string{
		"virtual-storage-1": {"primary", "secondary-1", "secondary-2"},
	}

	type matcher func(*testing.T, RepositoryMutatorRoute)

	requireOneOf := func(expected ...RepositoryMutatorRoute) matcher {
		return func(t *testing.T, actual RepositoryMutatorRoute) {
			sort.Slice(actual.Secondaries, func(i, j int) bool {
				return actual.Secondaries[i].Storage < actual.Secondaries[j].Storage
			})
			sort.Strings(actual.ReplicationTargets)
			require.Contains(t, expected, actual)
		}
	}

	primaryConn := &grpc.ClientConn{}
	secondary1Conn := &grpc.ClientConn{}
	secondary2Conn := &grpc.ClientConn{}

	db := testdb.New(t)

	for _, tc := range []struct {
		desc                        string
		setupRepoStore              func(t *testing.T, rs datastore.RepositoryStore)
		virtualStorage              string
		relativePath                string
		additionalRelativePath      string
		healthyNodes                StaticHealthChecker
		replicationFactor           int
		primaryPick                 int
		repositoryExists            bool
		expectedPrimaryCandidates   []int
		expectedSecondaryCandidates []int
		expectedRoute               matcher
		expectedErr                 error
	}{
		{
			desc:           "no healthy nodes",
			virtualStorage: "virtual-storage-1",
			relativePath:   "relative-path",
			healthyNodes:   StaticHealthChecker{},
			expectedErr:    ErrNoHealthyNodes,
		},
		{
			desc:           "invalid virtual storage",
			virtualStorage: "invalid",
			relativePath:   "relative-path",
			expectedErr:    nodes.ErrVirtualStorageNotExist,
		},
		{
			desc:                      "no healthy secondaries",
			virtualStorage:            "virtual-storage-1",
			relativePath:              "relative-path",
			healthyNodes:              StaticHealthChecker{"virtual-storage-1": {"primary"}},
			primaryPick:               0,
			expectedPrimaryCandidates: []int{1},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:       1,
					ReplicaPath:        storage.DeriveReplicaPath(1),
					Primary:            RouterNode{Storage: "primary", Connection: primaryConn},
					ReplicationTargets: []string{"secondary-1", "secondary-2"},
				},
			),
		},
		{
			desc:                      "success with all secondaries healthy",
			virtualStorage:            "virtual-storage-1",
			relativePath:              "relative-path",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			primaryPick:               0,
			expectedPrimaryCandidates: []int{3},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  storage.DeriveReplicaPath(1),
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
						{Storage: "secondary-2", Connection: secondary2Conn},
					},
				},
			),
		},
		{
			desc:                      "success with one secondary unhealthy",
			virtualStorage:            "virtual-storage-1",
			relativePath:              "relative-path",
			healthyNodes:              StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-1"}},
			primaryPick:               0,
			expectedPrimaryCandidates: []int{2},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  storage.DeriveReplicaPath(1),
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
					},
					ReplicationTargets: []string{"secondary-2"},
				},
			),
		},
		{
			desc:                      "replication factor of one configured",
			virtualStorage:            "virtual-storage-1",
			relativePath:              "relative-path",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			replicationFactor:         1,
			primaryPick:               0,
			expectedPrimaryCandidates: []int{3},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  storage.DeriveReplicaPath(1),
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
				},
			),
		},
		{
			desc:                        "replication factor of two configured",
			virtualStorage:              "virtual-storage-1",
			relativePath:                "relative-path",
			healthyNodes:                StaticHealthChecker(configuredNodes),
			replicationFactor:           2,
			primaryPick:                 0,
			expectedPrimaryCandidates:   []int{3},
			expectedSecondaryCandidates: []int{2},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  storage.DeriveReplicaPath(1),
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:  []RouterNode{{Storage: "secondary-1", Connection: secondary1Conn}},
				},
				RepositoryMutatorRoute{
					RepositoryID: 1,
					ReplicaPath:  storage.DeriveReplicaPath(1),
					Primary:      RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:  []RouterNode{{Storage: "secondary-2", Connection: secondary1Conn}},
				},
			),
		},
		{
			desc:                        "replication factor of three configured with unhealthy secondary",
			virtualStorage:              "virtual-storage-1",
			relativePath:                "relative-path",
			healthyNodes:                StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-1"}},
			replicationFactor:           3,
			primaryPick:                 0,
			expectedPrimaryCandidates:   []int{2},
			expectedSecondaryCandidates: []int{2},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:       1,
					ReplicaPath:        storage.DeriveReplicaPath(1),
					Primary:            RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:        []RouterNode{{Storage: "secondary-1", Connection: secondary1Conn}},
					ReplicationTargets: []string{"secondary-2"},
				},
			),
		},
		{
			desc: "repository already exists",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "relative-path", "something", "primary", nil, nil, true, true))
			},
			virtualStorage:            "virtual-storage-1",
			relativePath:              "relative-path",
			healthyNodes:              StaticHealthChecker(configuredNodes),
			primaryPick:               0,
			repositoryExists:          true,
			expectedPrimaryCandidates: []int{3},
			expectedErr:               fmt.Errorf("reserve repository id: %w", datastore.ErrRepositoryAlreadyExists),
		},
		{
			desc:                   "additional repository doesn't exist",
			virtualStorage:         "virtual-storage-1",
			relativePath:           "relative-path",
			additionalRelativePath: "non-existent",
			expectedErr: additionalRepositoryNotFoundError{
				storageName:  "virtual-storage-1",
				relativePath: "non-existent",
			},
		},
		{
			desc: "unhealthy primary with additional repository",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", nil, nil, true, true))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           StaticHealthChecker{"virtual-storage-1": {"secondary-1", "secondary-2"}},
			primaryPick:            1,
			expectedErr:            nodes.ErrPrimaryNotHealthy,
		},
		{
			desc: "additional repo without secondaries with explicit replication factor",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", nil, nil, true, true))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           StaticHealthChecker{"virtual-storage-1": {"primary"}},
			replicationFactor:      1,
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
				},
			),
		},
		{
			desc: "additional repo without secondaries",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", nil, nil, true, true))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           StaticHealthChecker{"virtual-storage-1": {"primary"}},
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
				},
			),
		},
		{
			desc: "additional repo without assignments",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", []string{
					"secondary-1", "secondary-2",
				}, nil, true, false))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           configuredNodes,
			primaryPick:            1,
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
						{Storage: "secondary-2", Connection: secondary2Conn},
					},
				},
			),
		},
		{
			desc: "additional repo with mixed-health secondaries",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", []string{
					"secondary-1", "secondary-2",
				}, nil, true, true))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           StaticHealthChecker{"virtual-storage-1": {"primary", "secondary-2"}},
			primaryPick:            1,
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:           []RouterNode{{Storage: "secondary-2", Connection: secondary2Conn}},
					ReplicationTargets:    []string{"secondary-1"},
				},
			),
		},
		{
			desc: "additional repo with mixed-generation secondaries",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", []string{
					"secondary-1",
					"secondary-2",
				}, nil, true, true))

				require.NoError(t, rs.SetGeneration(ctx, 1, "primary", "additional-relative-path", 10))
				require.NoError(t, rs.SetGeneration(ctx, 1, "secondary-1", "additional-relative-path", 3))
				require.NoError(t, rs.SetGeneration(ctx, 1, "secondary-2", "additional-relative-path", 10))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			healthyNodes:           configuredNodes,
			primaryPick:            0,
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries:           []RouterNode{{Storage: "secondary-2", Connection: secondary2Conn}},
					ReplicationTargets:    []string{"secondary-1"},
				},
			),
		},
		{
			desc: "additional repo ignores replication factor",
			setupRepoStore: func(t *testing.T, rs datastore.RepositoryStore) {
				require.NoError(t, rs.CreateRepository(ctx, 1, "virtual-storage-1", "additional-relative-path", "something", "primary", []string{
					"secondary-1",
					"secondary-2",
				}, nil, true, true))
			},
			virtualStorage:         "virtual-storage-1",
			additionalRelativePath: "additional-relative-path",
			replicationFactor:      2,
			primaryPick:            1,
			healthyNodes:           configuredNodes,
			expectedRoute: requireOneOf(
				RepositoryMutatorRoute{
					RepositoryID:          1,
					ReplicaPath:           storage.DeriveReplicaPath(1),
					AdditionalReplicaPath: "something",
					Primary:               RouterNode{Storage: "primary", Connection: primaryConn},
					Secondaries: []RouterNode{
						{Storage: "secondary-1", Connection: secondary1Conn},
						{Storage: "secondary-2", Connection: secondary2Conn},
					},
				},
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			if tc.setupRepoStore != nil {
				tc.setupRepoStore(t, rs)
			}

			var primaryCandidates, secondaryCandidates []int
			route, err := NewPerRepositoryRouter(
				Connections{
					"virtual-storage-1": {
						"primary":     primaryConn,
						"secondary-1": secondary1Conn,
						"secondary-2": secondary2Conn,
					},
				},
				nil,
				tc.healthyNodes,
				mockRandom{
					intnFunc: func(n int) int {
						primaryCandidates = append(primaryCandidates, n)
						return tc.primaryPick
					},
					shuffleFunc: func(n int, swap func(i, j int)) {
						secondaryCandidates = append(secondaryCandidates, n)
					},
				},
				nil,
				nil,
				rs,
				map[string]int{"virtual-storage-1": tc.replicationFactor},
			).RouteRepositoryCreation(ctx, tc.virtualStorage, tc.relativePath, tc.additionalRelativePath)

			require.Equal(t, tc.expectedPrimaryCandidates, primaryCandidates)
			require.Equal(t, tc.expectedSecondaryCandidates, secondaryCandidates)
			require.Equal(t, tc.expectedErr, err)

			if tc.expectedErr == nil {
				tc.expectedRoute(t, route)
			}
		})
	}
}
