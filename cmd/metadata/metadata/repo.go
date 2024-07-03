package metadata

import (
	"database/sql"

	"github.com/nnurry/pds/db"
	"github.com/nnurry/pds/metadata"
)

type filterRepo struct {
}

type InsertFilterPayload struct {
	Type           string
	Key            string
	MaxCardinality uint
	MaxFp          float64
	HashFuncNum    uint
	HashFuncType   string
	Blob           []byte
}

func NewFilterRepo() *filterRepo {
	return &filterRepo{}
}

func (r *filterRepo) CreateFilters(tx *sql.Tx, doCommit bool) error {
	query := `
	CREATE TABLE IF NOT EXISTS filters (
		key VARCHAR NOT NULL,
		created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('UTC', now()),
		type VARCHAR NOT NULL,
		max_cardinality INTEGER NOT NULL,
		max_fp REAL NOT NULL,
		hash_func_num BIGINT,
		hash_func_type VARCHAR,
		blob BYTEA,
		UNIQUE (type, key, max_cardinality, max_fp, hash_func_type)
	);`
	_, err := db.PostgresClient().Exec(query)

	if err != nil {
		tx.Rollback()
		return err
	}

	if doCommit {
		tx.Commit()
	}

	return nil
}

func (r *filterRepo) InsertFilter(tx *sql.Tx, doCommit bool, payload InsertFilterPayload) error {
	query := `
	INSERT INTO filters (type, key, max_cardinality, max_fp, hash_func_num, hash_func_type, blob)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (type, key, max_cardinality, max_fp, hash_func_type) DO UPDATE
	SET blob = EXCLUDED.blob
	`
	_, err := db.PostgresClient().Exec(
		query,
		payload.Type, payload.Key, payload.MaxCardinality,
		payload.MaxFp, payload.HashFuncNum, payload.HashFuncType,
		payload.Blob,
	)

	if err != nil {
		tx.Rollback()
		return err
	}

	if doCommit {
		tx.Commit()
	}

	return nil
}

type cardinalRepo struct {
}

func NewCardinalRepo() *cardinalRepo {
	return &cardinalRepo{}
}

type InsertCardinalPayload struct {
	Type string
	Key  string
	Blob []byte
}

func (r *cardinalRepo) CreateCardinals(tx *sql.Tx, doCommit bool) error {
	query := `
	CREATE TABLE IF NOT EXISTS cardinals (
		type VARCHAR NOT NULL,
		key VARCHAR NOT NULL,
		blob BYTA,
		UNIQUE (type, key)
	);`
	_, err := db.PostgresClient().Exec(query)

	if err != nil {
		tx.Rollback()
		return err
	}

	if doCommit {
		tx.Commit()
	}

	return nil
}

func (r *cardinalRepo) GetCardinal(payload InsertCardinalPayload) (metadata.Cardinal, error) {
	var err error

	query := `
	SELECT blob
	FROM cardinals
	WHERE type = $1 AND key = $2
	`

	err = db.PostgresClient().QueryRow(
		query,
		payload.Type, payload.Key,
	).Scan(&payload.Blob)

	if err != nil {
		return nil, err
	}

	var cardinal metadata.Cardinal

	switch payload.Type {
	case "STD_HLL":
		cardinal = metadata.NewStdHLL(14, false, payload.Key)
	case "REDIS_HLL":
		cardinal = metadata.NewRedisHLL(payload.Key)
	}

	err = cardinal.Deserialize(payload.Blob)

	if err != nil {
		return nil, err
	}

	return cardinal, nil
}

func (r *cardinalRepo) InsertCardinal(tx *sql.Tx, doCommit bool, payload InsertCardinalPayload) error {
	var err error

	query := `
	INSERT INTO cardinals (type, key, blob)
	VALUES ($1, $2, $3)
	ON CONFLICT (type, key) DO UPDATE
	SET blob = EXCLUDED.blob
	`
	_, err = db.PostgresClient().Exec(
		query,
		payload.Type, payload.Key, payload.Blob,
	)

	if err != nil {
		tx.Rollback()
		return err
	}

	if doCommit {
		tx.Commit()
	}

	return nil
}
