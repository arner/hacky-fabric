package storage

import (
	"errors"
)

// SimulationStore implements a very basic set of state interactions on a snapshot of the world state.
// It records reads and writes. By default, like in Fabric, you cannot 'read your own writes' but this can be configured.
// Function signatures correspond to the 'chaincode stub' where possible.
type SimulationStore struct {
	readOwnWrites bool
	namespace     string
	store         ReadStore
	blockNum      uint64
	reads         map[string]KVRead
	writes        map[string]KVWrite
}

type KVRead struct {
	Key     string
	Version *Version
}

type KVWrite struct {
	Key      string
	IsDelete bool
	Value    []byte
}

type Version struct {
	BlockNum uint64
	TxNum    uint64
}

type ReadStore interface {
	Get(string, string, uint64) (*WriteRecord, error)
}

// GetState behaves similar to in Fabric, with the exception that we _can_ read
// our own writes if explicitly configured.
// read own write (if enabled) -> return last written value, nil
// read own delete (if enabled) -> return nil, nil
// no result -> store read with nil version, return nil, nil
// deleted result -> no read, return nil, nil
// result -> store read version, return value, nil
func (s SimulationStore) GetState(key string) ([]byte, error) {
	if s.readOwnWrites {
		// return early, we don't record reading your own writes
		if record, ok := s.writes[key]; ok {
			if record.IsDelete {
				return nil, nil
			}
			return record.Value, nil
		}
	}

	// get from store snapshot
	record, err := s.store.Get(s.namespace, key, s.blockNum)
	if err != nil {
		return nil, err
	}

	var val []byte
	var read = KVRead{Key: key}
	if record != nil {
		// fabric doesn't add a read marker if the value is deleted.
		if record.IsDelete {
			return nil, nil
		}
		read.Version = &Version{
			BlockNum: record.BlockNum,
			TxNum:    record.TxNum,
		}
	}
	s.reads[key] = read

	return val, nil
}

// PutState puts the specified `key` and `value` into the transaction's
// writeset as a data-write proposal. PutState doesn't effect the ledger
// until the transaction is validated and successfully committed.
// Simple keys must not be an empty string and must not start with a
// null character (0x00) in order to avoid range query collisions with
// composite keys, which internally get prefixed with 0x00 as composite
// key namespace. In addition, if using CouchDB, keys can only contain
// valid UTF-8 strings and cannot begin with an underscore ("_").
func (s SimulationStore) PutState(key string, value []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	if len(value) == 0 {
		return errors.New("key is empty")
	}
	s.writes[key] = KVWrite{Key: key, Value: value}
	return nil
}

// DelState records the specified `key` to be deleted in the writeset of
// the transaction proposal. The `key` and its value will be deleted from
// the ledger when the transaction is validated and successfully committed.
func (s SimulationStore) DelState(key string) error {
	s.writes[key] = KVWrite{Key: key, IsDelete: true}
	return nil
}

type ReadWriteSet struct {
	Reads  []KVRead
	Writes []KVWrite
}

func (s *SimulationStore) Result() ReadWriteSet {
	rws := ReadWriteSet{
		Reads:  make([]KVRead, 0, len(s.reads)),
		Writes: make([]KVWrite, 0, len(s.writes)),
	}
	for _, r := range s.reads {
		rws.Reads = append(rws.Reads, r)
	}
	for _, w := range s.writes {
		rws.Writes = append(rws.Writes, w)
	}
	return rws
}

// Version is the blockheight of this snapshot.
func (s *SimulationStore) Version() uint64 {
	return s.blockNum
}
