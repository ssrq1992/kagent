// Package pgtest creates isolated databases on an explicitly configured test server.
package pgtest

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"net/url"
	"strings"
	"time"
)

// startDatabase creates one isolated database per fixture on an explicitly
// configured test server. No pre-existing database is truncated or reused.
func Start(ctx context.Context, dsn string) (string, func(), error) {
	connection, err := url.Parse(dsn)
	if err != nil || (connection.Scheme != "postgres" && connection.Scheme != "postgresql") {
		return "", nil, fmt.Errorf("test PostgreSQL DSN must be a postgres URL")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", nil, err
	}
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	defer admin.Close(context.WithoutCancel(ctx))
	name := "kagent_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return "", nil, err
	}
	connection.Path = "/" + name
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.ConnectConfig(cleanupCtx, cfg)
		if err != nil {
			fmt.Printf("test database cleanup connection failed: %v\n", err)
			return
		}
		defer conn.Close(cleanupCtx)
		if _, err = conn.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			fmt.Printf("test database cleanup failed: %v\n", err)
		}
	}
	return connection.String(), cleanup, nil
}
