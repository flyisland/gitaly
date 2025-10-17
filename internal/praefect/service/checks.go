package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v18/internal/praefect/nodes"
)

// Severity is a type that indicates the severity of a check
type Severity string

const (
	// Warning indicates a severity level of warning
	Warning Severity = "warning"
	// Fatal indicates a severity level of fatal
	// any checks that are Fatal will prevent Praefect from starting up
	Fatal = "fatal"
)

// Check represents a check to be performed to validate Gitaly cluster health.
// Checks are used to diagnose issues with Praefect cluster setup through the
// Praefect CLI `check` subcommand and also performed through the invoking of
// the readiness RPC.
type Check struct {
	Run         func(ctx context.Context) error
	Name        string
	Description string
	Severity    Severity
}

// CheckFunc is a function type that takes a praefect config and returns a Check
type CheckFunc func(conf config.Config, w io.Writer, quiet bool) *Check

// Checks returns slice of all checks that can be executed for praefect.
func Checks() []CheckFunc {
	return []CheckFunc{
		NewPraefectMigrationCheck,
		NewGitalyNodeConnectivityCheck,
		NewPostgresReadWriteCheck,
		NewUnavailableReposCheck,
	}
}

// NewPraefectMigrationCheck returns a Check that checks if all praefect migrations have run
func NewPraefectMigrationCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "praefect migrations",
		Description: "confirms whether or not all praefect migrations have run",
		Run: func(ctx context.Context) error {
			openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			db, err := glsql.OpenDB(openDBCtx, conf.DB)
			if err != nil {
				return err
			}
			defer db.Close()

			migrationSet := migrate.MigrationSet{
				TableName: migrations.MigrationTableName,
			}

			migrationSource := &migrate.MemoryMigrationSource{
				Migrations: migrations.All(),
			}

			migrationsNeeded, _, err := migrationSet.PlanMigration(db, "postgres", migrationSource, migrate.Up, 0)
			if err != nil {
				return err
			}

			missingMigrations := len(migrationsNeeded)

			if missingMigrations > 0 {
				return fmt.Errorf("%d migrations have not been run", missingMigrations)
			}

			return nil
		},
		Severity: Fatal,
	}
}

// NewGitalyNodeConnectivityCheck returns a check that ensures Praefect can talk to all nodes of all virtual storages
func NewGitalyNodeConnectivityCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name: "gitaly node connectivity & disk access",
		Description: "confirms if praefect can reach all of its gitaly nodes, and " +
			"whether or not the gitaly nodes can read/write from and to its storages.",
		Run: func(ctx context.Context) error {
			return nodes.PingAll(ctx, conf, nodes.NewTextPrinter(w), quiet)
		},
		Severity: Fatal,
	}
}

// NewPostgresReadWriteCheck returns a check that ensures Praefect can read and write to the database
func NewPostgresReadWriteCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "database read/write",
		Description: "checks if praefect can write/read to and from the database",
		Run: func(ctx context.Context) error {
			db, err := glsql.OpenDB(ctx, conf.DB)
			if err != nil {
				return fmt.Errorf("error opening database connection: %w", err)
			}
			defer db.Close()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("error starting transaction: %w", err)
			}
			//nolint:errcheck
			defer tx.Rollback()

			var id int
			if err = tx.QueryRowContext(ctx, "SELECT id FROM hello_world").Scan(&id); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("error reading from table: %w", err)
				}
			}

			logMessage(quiet, w, "successfully read from database")

			res, err := tx.ExecContext(ctx, "INSERT INTO hello_world (id) VALUES(1)")
			if err != nil {
				return fmt.Errorf("error writing to table: %w", err)
			}

			rows, err := res.RowsAffected()
			if err != nil {
				return err
			}

			if rows != 1 {
				return errors.New("failed to insert row")
			}
			logMessage(quiet, w, "successfully wrote to database")

			return nil
		},
		Severity: Fatal,
	}
}

// NewUnavailableReposCheck returns a check that finds the number of repositories without a valid primary
func NewUnavailableReposCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "unavailable repositories",
		Description: "lists repositories that are missing a valid primary, hence rendering them unavailable",
		Run: func(ctx context.Context) error {
			db, err := glsql.OpenDB(ctx, conf.DB)
			if err != nil {
				return fmt.Errorf("error opening database connection: %w", err)
			}
			defer db.Close()

			unavailableRepositories, err := datastore.CountUnavailableRepositories(
				ctx,
				db,
				conf.VirtualStorageNames(),
			)
			if err != nil {
				return err
			}

			if len(unavailableRepositories) == 0 {
				logMessage(quiet, w, "All repositories are available.")
				return nil
			}

			for virtualStorage, unavailableCount := range unavailableRepositories {
				format := "virtual-storage %q has %d repositories that are unavailable."
				if unavailableCount == 1 {
					format = "virtual-storage %q has %d repository that is unavailable."
				}
				logMessage(quiet, w, format, virtualStorage, unavailableCount)
			}

			return errors.New("repositories unavailable")
		},
		Severity: Warning,
	}
}

func logMessage(quiet bool, w io.Writer, format string, a ...interface{}) {
	if quiet {
		return
	}
	fmt.Fprintf(w, format+"\n", a...)
}
