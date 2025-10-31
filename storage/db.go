package storage

import (
	"database/sql"
	"fmt"
)

// VersionedDB provides persistence for read/write sets per channel.
type VersionedDB struct {
	channel string
	table   string
	backend *sql.DB
}

func New(channel string, db *sql.DB) *VersionedDB {
	return &VersionedDB{
		channel: channel,
		table:   fmt.Sprintf("worldstate_%s", channel),
		backend: db,
	}
}

// WriteRecord represents a single write or delete in the world state.
type WriteRecord struct {
	Namespace string
	Key       string
	BlockNum  uint64
	TxNum     uint64
	Value     []byte
	IsDelete  bool
	TxID      string
}

// Init creates the world state table for a channel if it doesn't exist.
func (s *VersionedDB) Init() error {
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
	`, s.table, s.channel, s.table, s.channel, s.table)

	_, err := s.backend.Exec(schema)
	if err != nil {
		return fmt.Errorf("init table %s: %w", s.table, err)
	}
	return nil
}

// InsertWrite inserts a single versioned key/value write into the channel table.
// It is idempotent — if the same (namespace,key,block,tx) already exists, it's ignored.
func (s *VersionedDB) InsertWrite(w WriteRecord) error {
	query := fmt.Sprintf(`
	INSERT OR IGNORE INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7);
	`, s.table)

	_, err := s.backend.Exec(query,
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
func (s *VersionedDB) BatchInsert(writes []WriteRecord) error {
	if len(writes) == 0 {
		return nil
	}
	tx, err := s.backend.Begin()
	if err != nil {
		return fmt.Errorf("begin batch insert: %w", err)
	}
	defer tx.Rollback()

	var stmt *sql.Stmt
	stmt, err = tx.Prepare(fmt.Sprintf(`
	INSERT INTO %s (namespace, key, version_block, version_tx, value, is_delete, tx_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (namespace, key, version_block, version_tx) DO NOTHING;

	`, s.table))
	if err != nil {
		return fmt.Errorf("prepare batch insert: %w", err)
	}
	defer stmt.Close()

	for _, w := range writes {
		if _, err := stmt.Exec(w.Namespace, w.Key, w.BlockNum, w.TxNum, w.Value, w.IsDelete, w.TxID); err != nil {
			return fmt.Errorf("batch insert exec: %w", err)
		}
	}
	if err := s.MarkProcessed(tx, writes[0].BlockNum); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch insert: %w", err)
	}
	return nil
}

func (s *VersionedDB) MarkProcessed(tx *sql.Tx, blockNum uint64) error {
	query := `
	INSERT INTO channel_progress(channel, last_block)
	VALUES (?, ?)
	ON CONFLICT(channel) DO UPDATE SET last_block = excluded.last_block
	WHERE excluded.last_block > channel_progress.last_block;
	`
	var err error
	if tx == nil {
		_, err = s.backend.Exec(query, s.channel, blockNum)
	} else {
		_, err = tx.Exec(query, s.channel, blockNum)
	}
	if err != nil {
		return fmt.Errorf("update last block exec: %w", err)
	}
	return nil

}

// Get returns the version of a key at a certain time.
func (s *VersionedDB) Get(namespace, key string, lastBlock uint64) (*WriteRecord, error) {
	query := fmt.Sprintf(`
	SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
	FROM %s
	WHERE namespace = $1 AND key = $2 AND version_block <= $3
	ORDER BY version_block DESC, version_tx DESC
	LIMIT 1;
	`, s.table)

	row := s.backend.QueryRow(query, namespace, key, lastBlock)
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
func (s *VersionedDB) GetCurrent(namespace, key string) (*WriteRecord, error) {
	query := fmt.Sprintf(`
	SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
	FROM %s
	WHERE namespace = $1 AND key = $2
	ORDER BY version_block DESC, version_tx DESC
	LIMIT 1;
	`, s.table)

	row := s.backend.QueryRow(query, namespace, key)
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
func (s *VersionedDB) GetHistory(namespace, key string) ([]WriteRecord, error) {
	query := fmt.Sprintf(`
	SELECT namespace, key, version_block, version_tx, value, is_delete, tx_id
	FROM %s
	WHERE namespace = $1 AND key = $2
	ORDER BY version_block, version_tx;
	`, s.table)

	rows, err := s.backend.Query(query, namespace, key)
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
func (s *VersionedDB) LastProcessedBlock() (uint64, error) {
	var lastBlock sql.NullInt64
	err := s.backend.QueryRow("SELECT last_block FROM channel_progress WHERE channel = $1", s.channel).Scan(&lastBlock)
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

// GetSimulationStore returns a read only snapshot that records reads and writes.
// readOwnWrites is false in Fabric, but expected by Ethereum smart contracts. It means that a transaction can put or delete a value,
// and read it back within the same transaction. This reading back is not recorded as a read in the read/write set.
func (s *VersionedDB) NewSimulationStore(namespace string, blockNum uint64, readOwnWrites bool) (SimulationStore, error) {
	version := blockNum
	if blockNum == 0 {
		lastBlock, err := s.LastProcessedBlock()
		if err != nil {
			return SimulationStore{}, err
		}
		version = lastBlock
	}

	return SimulationStore{
		namespace:     namespace,
		store:         s,
		blockNum:      version,
		reads:         make(map[string]KVRead),
		writes:        make(map[string]KVWrite),
		readOwnWrites: readOwnWrites,
	}, nil
}
