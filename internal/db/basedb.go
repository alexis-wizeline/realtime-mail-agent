package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	emptyConnString = errors.New("connection string is empty")
)

type DbComposedErr struct {
	message string
	err     error
}

func (d *DbComposedErr) Error() string {
	return fmt.Sprintf("%s, db error: %s", d.message, d.err)
}

type DBX interface {
	realtimemailsql.DBTX

	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Ping(context.Context) error

	Close()
}

func SetupDB(ctx context.Context) (DBX, error) {
	connStr := os.Getenv("DATABASE_URL")
	if len(strings.Trim(connStr, " ")) == 0 {
		return nil, emptyConnString
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, &DbComposedErr{
			message: "unable to create the connection pool",
			err:     err,
		}
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, &DbComposedErr{
			message: "Unable to Connect to the db",
			err:     err,
		}
	}

	return pool, nil
}
