package committer

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
)

type mockStore struct {
	lastBlock uint64
	getFn     func(ns, key string, block uint64) (*WriteRecord, error)
}

func (m *mockStore) LastProcessedBlock() (uint64, error) {
	return m.lastBlock, nil
}

func (m *mockStore) Get(ns, key string, block uint64) (*WriteRecord, error) {
	if m.getFn != nil {
		return m.getFn(ns, key, block)
	}
	return nil, nil
}

func TestGetState(t *testing.T) {
	tests := []struct {
		name      string
		record    *WriteRecord
		getErr    error
		wantValue []byte
		wantErr   bool
	}{
		{
			name:      "key exists",
			record:    &WriteRecord{Key: "k1", Value: []byte("v1"), BlockNum: 10, TxNum: 1},
			wantValue: []byte("v1"),
		},
		{
			name:    "key missing",
			record:  nil,
			wantErr: false,
		},
		{
			name:    "error fetching key",
			getErr:  errors.New("db error"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockStore{
				getFn: func(ns, key string, block uint64) (*WriteRecord, error) {
					return tt.record, tt.getErr
				},
			}

			stub := Stub{
				namespace: "ns",
				store:     store,
				block:     100,
				reads:     make(map[string]*kvrwset.KVRead),
				writes:    make(map[string]*kvrwset.KVWrite),
			}

			val, err := stub.GetState("k1")
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(val, tt.wantValue) {
				t.Errorf("expected value %v, got %v", tt.wantValue, val)
			}
		})
	}
}

func TestPutStateAndDelState(t *testing.T) {
	stub := Stub{
		namespace: "ns",
		store:     nil,
		block:     1,
		reads:     make(map[string]*kvrwset.KVRead),
		writes:    make(map[string]*kvrwset.KVWrite),
	}

	cases := []struct {
		name        string
		operations  func(s *Stub) error
		expectKeys  []string
		expectDel   map[string]bool
		expectValue map[string][]byte
		expectErr   bool
	}{
		{
			name: "single put",
			operations: func(s *Stub) error {
				return s.PutState("a", []byte("A"))
			},
			expectKeys:  []string{"a"},
			expectValue: map[string][]byte{"a": []byte("A")},
			expectDel:   map[string]bool{"a": false},
		},
		{
			name: "put empty key",
			operations: func(s *Stub) error {
				return s.PutState("", []byte("X"))
			},
			expectErr: true,
		},
		{
			name: "delete key",
			operations: func(s *Stub) error {
				return s.DelState("z")
			},
			expectKeys: []string{"z"},
			expectDel:  map[string]bool{"z": true},
		},
		{
			name: "multiple writes same key",
			operations: func(s *Stub) error {
				s.PutState("x", []byte("v1"))
				s.PutState("x", []byte("v2")) // overwrite
				return nil
			},
			expectKeys:  []string{"x"},
			expectValue: map[string][]byte{"x": []byte("v2")},
		},
		{
			name: "put then delete same key",
			operations: func(s *Stub) error {
				s.PutState("y", []byte("v"))
				return s.DelState("y")
			},
			expectKeys: []string{"y"},
			expectDel:  map[string]bool{"y": true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := stub
			s.writes = make(map[string]*kvrwset.KVWrite)

			err := tc.operations(&s)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(s.writes) != len(tc.expectKeys) {
				t.Fatalf("expected %d writes, got %d", len(tc.expectKeys), len(s.writes))
			}

			for _, key := range tc.expectKeys {
				w := s.writes[key]
				if w == nil {
					t.Fatalf("expected key %s in writes", key)
				}
				if tc.expectDel[key] && !w.IsDelete {
					t.Errorf("expected %s to be deleted", key)
				}
				if val, ok := tc.expectValue[key]; ok && !reflect.DeepEqual(w.Value, val) {
					t.Errorf("expected value %v, got %v", val, w.Value)
				}
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	stub := Stub{
		reads: map[string]*kvrwset.KVRead{
			"k1": {Key: "k1"},
			"k2": {Key: "k2"},
		},
		writes: map[string]*kvrwset.KVWrite{
			"k3": {Key: "k3", Value: []byte("v3")},
		},
	}

	rwset := stub.Finalize()
	if len(rwset.Reads) != 2 {
		t.Errorf("expected 2 reads, got %d", len(rwset.Reads))
	}
	if len(rwset.Writes) != 1 {
		t.Errorf("expected 1 write, got %d", len(rwset.Writes))
	}
}
