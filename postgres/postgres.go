package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yogenyslav/logger"
)

var (
	TimeoutExceeded = errors.New("postgres connection timeout exceeded")
)

func MustNew(cfg *Config, retryTimeout int) *pgxpool.Pool {
	var (
		err         error
		pool        *pgxpool.Pool
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(retryTimeout)*time.Second)
		url         = "postgres://%s:%s@%s:%d/%s?sslmode="
	)
	defer cancel()

	if cfg.Ssl {
		url += "enable"
	} else {
		url += "disable"
	}
	connString := fmt.Sprintf(url, cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Db)

	for ctx.Err() == nil {
		time.Sleep(time.Second)
		pool, err = pgxpool.New(ctx, connString)
		if err != nil {
			logger.Errorf("failed to open new pool: %v", err)
			continue
		}

		if err = pool.Ping(ctx); err != nil {
			logger.Errorf("can't access postgres: %v", err)
			continue
		}

		return pool
	}
	logger.Panic(TimeoutExceeded)
	return nil
}

func QueryStruct[T any](ctx context.Context, pg *pgxpool.Pool, query string, args ...any) (T, error) {
	var (
		res  T
		rows pgx.Rows
		err  error
	)
	rows, err = pg.Query(ctx, query, args...)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	res, err = pgx.CollectExactlyOneRow[T](rows, pgx.RowToStructByName[T])
	return res, err
}

func QueryStructSlice[T any](ctx context.Context, pg *pgxpool.Pool, query string, args ...any) ([]T, error) {
	var (
		res  []T
		rows pgx.Rows
		err  error
	)
	rows, err = pg.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res, err = pgx.CollectRows[T](rows, pgx.RowToStructByName[T])
	return res, err
}

func QueryPrimitive[T any](ctx context.Context, pg *pgxpool.Pool, query string, args ...any) (T, error) {
	var (
		res  T
		rows pgx.Rows
		err  error
	)
	rows, err = pg.Query(ctx, query, args...)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	res, err = pgx.CollectExactlyOneRow[T](rows, pgx.RowTo[T])
	return res, err
}

func QueryPrimitiveSlice[T any](ctx context.Context, pg *pgxpool.Pool, query string, args ...any) ([]T, error) {
	var (
		res  []T
		rows pgx.Rows
		err  error
	)
	rows, err = pg.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res, err = pgx.CollectRows[T](rows, pgx.RowTo[T])
	return res, err
}
