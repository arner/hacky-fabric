package committer

import (
	"errors"

	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
)

type StubFactory struct {
	namespace string
	store     WorldStateStore
}

type Stub struct {
	namespace string
	store     WorldStateStore
	// block is the last processed block, to make sure all reads in this context are consistent (from the same snapshot).
	block  uint64
	reads  map[string]*kvrwset.KVRead
	writes map[string]*kvrwset.KVWrite
}

type WorldStateStore interface {
	LastProcessedBlock() (uint64, error)
	Get(string, string, uint64) (*WriteRecord, error)
}

func (s StubFactory) New() (Stub, error) {
	lastBlock, err := s.store.LastProcessedBlock()
	if err != nil {
		return Stub{}, err
	}

	return Stub{
		namespace: s.namespace,
		store:     s.store,
		block:     lastBlock,
	}, nil
}

// Finalize deduplicates the reads after all reads have been added
func (s *Stub) Finalize() *kvrwset.KVRWSet {
	reads := make([]*kvrwset.KVRead, 0, len(s.reads))
	for _, r := range s.reads {
		reads = append(reads, r)
	}

	writes := make([]*kvrwset.KVWrite, 0, len(s.writes))
	for _, w := range s.writes {
		writes = append(writes, w)
	}

	return &kvrwset.KVRWSet{
		Reads:  reads,
		Writes: writes,
	}
}

// GetState returns the value of the specified `key` from the
// ledger. Note that GetState doesn't read data from the writeset, which
// has not been committed to the ledger. In other words, GetState doesn't
// consider data modified by PutState that has not been committed.
// If the key does not exist in the state database, (nil, nil) is returned.
func (s Stub) GetState(key string) ([]byte, error) {
	read, err := s.store.Get(s.namespace, key, s.block)
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, nil
	}
	s.reads[read.Key] = &kvrwset.KVRead{
		Key: read.Key,
		Version: &kvrwset.Version{
			BlockNum: read.BlockNum,
			TxNum:    uint64(read.TxNum),
		},
	}
	return read.Value, nil
}

// PutState puts the specified `key` and `value` into the transaction's
// writeset as a data-write proposal. PutState doesn't effect the ledger
// until the transaction is validated and successfully committed.
// Simple keys must not be an empty string and must not start with a
// null character (0x00) in order to avoid range query collisions with
// composite keys, which internally get prefixed with 0x00 as composite
// key namespace. In addition, if using CouchDB, keys can only contain
// valid UTF-8 strings and cannot begin with an underscore ("_").
func (s Stub) PutState(key string, value []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	s.writes[key] = &kvrwset.KVWrite{Key: key, Value: value}
	return nil
}

// DelState records the specified `key` to be deleted in the writeset of
// the transaction proposal. The `key` and its value will be deleted from
// the ledger when the transaction is validated and successfully committed.
func (s Stub) DelState(key string) error {
	s.writes[key] = &kvrwset.KVWrite{Key: key, IsDelete: true}
	return nil
}
