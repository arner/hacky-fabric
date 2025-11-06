package storage

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
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

			stub := SimulationStore{
				namespace: "ns",
				store:     store,
				blockNum:  100,
				reads:     make(map[string]KVRead),
				writes:    make(map[string]KVWrite),
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

func newStub() SimulationStore {
	return SimulationStore{
		namespace: "ns",
		store:     nil,
		blockNum:  1,
		reads:     make(map[string]KVRead),
		writes:    make(map[string]KVWrite),
	}
}

func TestPutStateAndDelState(t *testing.T) {
	DELETED := []byte("DELETED")
	cases := []struct {
		name        string
		operations  func(s *SimulationStore) error
		expectWrite map[string][]byte
		expectRead  map[string]bool
		expectErr   bool
	}{
		{
			name: "single put",
			operations: func(s *SimulationStore) error {
				return s.PutState("a", []byte("A"))
			},
			expectRead:  map[string]bool{"a": false},
			expectWrite: map[string][]byte{"a": []byte("A")},
		},
		{
			name: "put empty key is not allowed",
			operations: func(s *SimulationStore) error {
				return s.PutState("", []byte("X"))
			},
			expectErr: true,
		},
		{
			name: "put empty value is not allowed",
			operations: func(s *SimulationStore) error {
				return s.PutState("z", nil)
			},
			expectErr: true,
		},
		{
			name: "delete key",
			operations: func(s *SimulationStore) error {
				return s.DelState("z")
			},
			expectRead:  map[string]bool{"z": false},
			expectWrite: map[string][]byte{"z": DELETED},
		},
		{
			name: "multiple writes same key only keeps the second",
			operations: func(s *SimulationStore) error {
				s.PutState("x", []byte("v1"))
				s.PutState("x", []byte("v2")) // overwrite
				return nil
			},
			expectRead:  map[string]bool{"x": false},
			expectWrite: map[string][]byte{"x": []byte("v2")},
		},
		{
			name: "put then delete same key",
			operations: func(s *SimulationStore) error {
				s.PutState("y", []byte("v"))
				return s.DelState("y")
			},
			expectRead:  map[string]bool{"y": false},
			expectWrite: map[string][]byte{"y": DELETED},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStub()

			// errors
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
			if len(s.writes) != len(tc.expectWrite) {
				t.Fatalf("expected %d writes, got %d", len(tc.expectWrite), len(s.writes))
			}

			// writes
			for key, val := range tc.expectWrite {
				w := s.writes[key]
				if bytes.Equal(val, DELETED) {
					if !w.IsDelete {
						t.Errorf("expected %s to be deleted", key)
					}
				} else if !bytes.Equal(val, w.Value) {
					t.Errorf("expected value %v, got %v", val, w.Value)
				}
			}

			// reads
			for key, expected := range tc.expectRead {
				_, found := s.reads[key]
				if expected && !found {
					t.Errorf("expected %s to be read", key)
				}
				if !expected && found {
					t.Errorf("expected %s to not be read", key)
				}
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	stub := SimulationStore{
		reads: map[string]KVRead{
			"k1": {Key: "k1"},
			"k2": {Key: "k2", Version: &Version{BlockNum: 1, TxNum: 0}},
		},
		writes: map[string]KVWrite{
			"k3": {Key: "k3", Value: []byte("v3")},
		},
	}
	rws := stub.Result()
	if len(rws.Reads) != 2 {
		t.Errorf("expected 2 reads, got %d", len(rws.Reads))
	}
	if len(rws.Writes) != 1 {
		t.Errorf("expected 1 write, got %d", len(rws.Writes))
	}
}
