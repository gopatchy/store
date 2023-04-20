package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gopatchy/metadata"
	// Register sqlite3 db handler.
	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

func NewStore(conn string) (*Store, error) {
	db, err := sql.Open("sqlite3", conn)
	if err != nil {
		return nil, err
	}

	// TODO: Keep a set of prepared statements with PrepareContext()
	// TODO: Consider tuning per https://phiresky.github.io/blog/2020/sqlite-performance-tuning/

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) Write(ctx context.Context, t string, obj any) error {
	id := metadata.GetMetadata(obj).ID

	js, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	err = s.exec(ctx, "INSERT INTO `%s` (id, obj) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET obj=?;", t, id, js, js)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, t, id string) error {
	err := s.exec(ctx, "DELETE FROM `%s` WHERE id=?", t, id)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Read(ctx context.Context, t, id string, factory func() any) (any, error) {
	rows, err := s.query(ctx, "SELECT obj FROM `%s` WHERE id=?;", t, id)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var js []byte

	err = rows.Scan(&js)
	if err != nil {
		return nil, err
	}

	obj := factory()

	err = json.Unmarshal(js, obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (s *Store) List(ctx context.Context, t string, factory func() any) ([]any, error) {
	rows, err := s.query(ctx, "SELECT obj FROM `%s`;", t)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ret := []any{}

	for rows.Next() {
		var js []byte

		err = rows.Scan(&js)
		if err != nil {
			return nil, err
		}

		obj := factory()

		err = json.Unmarshal(js, obj)
		if err != nil {
			return nil, err
		}

		ret = append(ret, obj)
	}

	return ret, nil
}

func (s *Store) exec(ctx context.Context, query, t string, args ...any) error {
	query = fmt.Sprintf(query, t)

	_, err := s.db.ExecContext(ctx, query, args...)
	if err == nil {
		return nil
	}

	_, err = s.db.ExecContext(ctx, s.tableSQL(t))
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) query(ctx context.Context, query, t string, args ...any) (*sql.Rows, error) {
	query = fmt.Sprintf(query, t)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err == nil {
		return rows, nil
	}

	_, err = s.db.ExecContext(ctx, s.tableSQL(t))
	if err != nil {
		return nil, err
	}

	return s.db.QueryContext(ctx, query, args...)
}

func (s *Store) tableSQL(t string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id TEXT NOT NULL PRIMARY KEY, obj TEXT NOT NULL);", t)
}
