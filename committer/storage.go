package committer

import (
	"database/sql"
	"fmt"
)

// WriteRecord represents a single write or delete in the world state.
type WriteRecord struct {
	Namespace string
	Key       string
	BlockNum  uint64
	TxNum     int
	Value     []byte
	IsDelete  bool
	TxID      string
}

// Store provides persistence for read/write sets per channel.
type Store struct {
	channel string
	table   string
	DB      *sql.DB
	Backend string // "postgres" or "sqlite"
}

func NewStorage(channel string, db *sql.DB, backend string) *Store {
	return &Store{
		channel: channel,
		table:   fmt.Sprintf("worldstate_%s", channel),
		DB:      db,
		Backend: backend,
	}
}

// Init creates the world state table for a channel if it doesn't exist.
func (sp *Store) Init() error {
	schema := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	namespace TEXT NOT NULL,
	key TEXT NOT NULL,
	version_block BIGINT NOT NULL,
	version_tx INTEGER NOT NULL,
	value BLOB,
	is_delete BOOLEAN NOT NULL DEFAULT false,
	tx_id TEXT NOT NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (namespace, key, version_block, version_tx)
);
CREATE INDEX IF NOT EXISTS idx_%s_ns_key ON %s (namespace, key);
CREATE INDEX IF NOT EXISTS idx_%s_block_tx ON %s (version_block, version_tx);

CREATE TABLE IF NOT EXISTS channel_progress (
	channel TEXT PRIMARY KEY,
	last_block BIGINT NOT NULL
);
`, sp.table, sp.channel, sp.table, sp.channel, sp.table)

	_, err := sp.DB.Exec(schema)
	if err != nil {
		return fmt.Errorf("init table %s: %w", sp.table, err)
	}
	return nil
}

// InsertWrite inserts a single versioned key/value write into the channel table.
// It is idempotent — if the same (namespace,key,block,tx) already exists, it's ignored.
func (sp *Store) InsertWrite(w WriteRecord) error {
	var query string
	switch sp.Backend {
	case "postgres":
		query = fmt.Sprintf(`
INSERT INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (namespace, key, version_block, version_tx) DO NOTHING;
`, sp.table)
	case "sqlite":
		query = fmt.Sprintf(`
INSERT OR IGNORE INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
VALUES (?, ?, ?, ?, ?, ?, ?);
`, sp.table)
	default:
		return fmt.Errorf("unsupported backend: %s", sp.Backend)
	}

	_, err := sp.DB.Exec(query,
		w.Namespace,
		w.Key,
		w.BlockNum,
		w.TxNum,
		w.Value,
		w.IsDelete,
		w.TxID,
	)
	if err != nil {
		return fmt.Errorf("insert write: %w", err)
	}
	return nil
}

// BatchInsert performs multiple inserts efficiently in a single transaction.
func (sp *Store) BatchInsert(writes []WriteRecord) error {
	if len(writes) == 0 {
		return nil
	}
	tx, err := sp.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin batch insert: %w", err)
	}
	defer tx.Rollback()

	var stmt *sql.Stmt
	switch sp.Backend {
	case "postgres":
		stmt, err = tx.Prepare(fmt.Sprintf(`
INSERT INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (namespace, key, version_block, version_tx) DO NOTHING;
`, sp.table))
	case "sqlite":
		stmt, err = tx.Prepare(fmt.Sprintf(`
INSERT OR IGNORE INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
VALUES (?, ?, ?, ?, ?, ?, ?);
`, sp.table))
	default:
		return fmt.Errorf("unsupported backend: %s", sp.Backend)
	}
	if err != nil {
		return fmt.Errorf("prepare batch insert: %w", err)
	}
	defer stmt.Close()

	for _, w := range writes {
		if _, err := stmt.Exec(w.Namespace, w.Key, w.BlockNum, w.TxNum, w.Value, w.IsDelete, w.TxID); err != nil {
			return fmt.Errorf("batch insert exec: %w", err)
		}
	}
	if err := sp.MarkProcessed(tx, writes[0].BlockNum); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch insert: %w", err)
	}
	return nil
}

func (sp *Store) MarkProcessed(tx *sql.Tx, blockNum uint64) error {
	// last block
	var query string
	switch sp.Backend {
	case "postgres":
		query = `
INSERT INTO channel_progress(channel, last_block)
VALUES ($1, $2)
ON CONFLICT (channel) DO UPDATE SET last_block = EXCLUDED.last_block
WHERE EXCLUDED.last_block > channel_progress.last_block;
`
	case "sqlite":
		query = `
INSERT INTO channel_progress(channel, last_block)
VALUES (?, ?)
ON CONFLICT(channel) DO UPDATE SET last_block = excluded.last_block
WHERE excluded.last_block > channel_progress.last_block;
`
	default:
		return fmt.Errorf("unsupported backend: %s", sp.Backend)
	}

	var err error
	if tx == nil {
		_, err = sp.DB.Exec(query, sp.channel, blockNum)
	} else {
		_, err = tx.Exec(query, sp.channel, blockNum)
	}
	if err != nil {
		return fmt.Errorf("update last block exec: %w", err)
	}
	return nil

}

// Get returns the version of a key at a certain time
func (sp *Store) Get(namespace, key string, lastBlock uint64) (*WriteRecord, error) {
	query := fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = ? AND key = ? AND version_block <= ?
ORDER BY version_block DESC, version_tx DESC
LIMIT 1;
`, sp.table)

	if sp.Backend == "postgres" {
		query = fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = $1 AND key = $2 AND version_block <= $3
ORDER BY version_block DESC, version_tx DESC
LIMIT 1;
`, sp.table)
	}

	row := sp.DB.QueryRow(query, namespace, key, lastBlock)
	var w WriteRecord
	if err := row.Scan(&w.Namespace, &w.Key, &w.BlockNum, &w.TxNum, &w.Value, &w.IsDelete, &w.TxID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get current: %w", err)
	}
	return &w, nil
}

// GetCurrent returns the latest version of a key in a namespace.
func (sp *Store) GetCurrent(namespace, key string) (*WriteRecord, error) {
	query := fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = ? AND key = ?
ORDER BY version_block DESC, version_tx DESC
LIMIT 1;
`, sp.table)

	if sp.Backend == "postgres" {
		query = fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = $1 AND key = $2
ORDER BY version_block DESC, version_tx DESC
LIMIT 1;
`, sp.table)
	}

	row := sp.DB.QueryRow(query, namespace, key)
	var w WriteRecord
	if err := row.Scan(&w.Namespace, &w.Key, &w.BlockNum, &w.TxNum, &w.Value, &w.IsDelete, &w.TxID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get current: %w", err)
	}
	return &w, nil
}

// GetHistory returns all versions of a key ordered by version.
func (sp *Store) GetHistory(namespace, key string) ([]WriteRecord, error) {
	query := fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = ? AND key = ?
ORDER BY version_block, version_tx;
`, sp.table)

	if sp.Backend == "postgres" {
		query = fmt.Sprintf(`
SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
FROM %s
WHERE namespace = $1 AND key = $2
ORDER BY version_block, version_tx;
`, sp.table)
	}

	rows, err := sp.DB.Query(query, namespace, key)
	if err != nil {
		return nil, fmt.Errorf("get history: %w", err)
	}
	defer rows.Close()

	var result []WriteRecord
	for rows.Next() {
		var w WriteRecord
		if err := rows.Scan(&w.Namespace, &w.Key, &w.BlockNum, &w.TxNum, &w.Value, &w.IsDelete, &w.TxID); err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		result = append(result, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate history: %w", err)
	}
	return result, nil
}

// LastProcessedBlock returns the highest block number stored for the given channel.
// Returns 0 if there are no writes yet.
func (sp *Store) LastProcessedBlock() (uint64, error) {
	query := `SELECT last_block FROM channel_progress WHERE channel = ?`
	if sp.Backend == "postgres" {
		query = `SELECT last_block FROM channel_progress WHERE channel = $1`
	}

	var lastBlock sql.NullInt64
	err := sp.DB.QueryRow(query, sp.channel).Scan(&lastBlock)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("query last processed block: %w", err)
	}
	if !lastBlock.Valid {
		return 0, nil
	}
	return uint64(lastBlock.Int64), nil
}
