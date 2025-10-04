package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/arner/hacky-fabric/committer"
	"github.com/arner/hacky-fabric/fabrictx"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
)

const (
	Channel   = "mychannel"
	Chaincode = "basicts"
)

type testLogger struct {
	t *testing.T
}

func (tl testLogger) Printf(format string, v ...any) {
	tl.t.Logf(format, v...)
}

const SqliteConn = "file::memory:?cache=shared"

func TestClientTransactions(t *testing.T) {
	c, err := NewClient(t.Context(), "keys", SqliteConn, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// process blocks in the background
	go c.Committer.Run()

	// make sure the committer is synced before we start
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	err = c.Committer.WaitUntilSynced(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	height, err := c.Committer.PeerBlockHeight()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("initial blockheight: %d", height)

	key := rand.Text()
	lastVal := rand.Text()

	tests := []struct {
		name               string
		rw                 *kvrwset.KVRWSet
		expected           peer.TxValidationCode
		waitForPropagation bool
	}{
		{
			name:     "blind write",
			expected: peer.TxValidationCode_VALID,
			rw: &kvrwset.KVRWSet{
				Writes: []*kvrwset.KVWrite{
					{Key: key, Value: []byte(`hello world`)},
				},
			},
		},
		{
			name:               "read previous key and write a new one",
			expected:           peer.TxValidationCode_VALID,
			waitForPropagation: true,
			rw: &kvrwset.KVRWSet{
				Reads: []*kvrwset.KVRead{
					{
						Key: key,
						Version: &kvrwset.Version{
							BlockNum: height,
							TxNum:    0,
						},
					},
				},
				Writes: []*kvrwset.KVWrite{
					{Key: key + "new", Value: []byte(`hello`)},
				},
			},
		},
		{
			name:     "read nonexistent key should fail",
			expected: peer.TxValidationCode_MVCC_READ_CONFLICT,
			rw: &kvrwset.KVRWSet{
				Reads: []*kvrwset.KVRead{
					{
						Key: "NONEXISTENT",
						Version: &kvrwset.Version{
							BlockNum: 0,
							TxNum:    0,
						},
					},
				},
				Writes: []*kvrwset.KVWrite{
					{Key: key + "no", Value: []byte(`should fail`)},
				},
			},
		},
		{
			name:     "read and write same key",
			expected: peer.TxValidationCode_VALID,
			rw: &kvrwset.KVRWSet{
				Reads: []*kvrwset.KVRead{
					{
						Key: key,
						Version: &kvrwset.Version{
							BlockNum: height,
							TxNum:    0,
						},
					},
				},
				Writes: []*kvrwset.KVWrite{
					{Key: key, Value: []byte(lastVal)},
				},
			},
		},
		{
			name:               "read old version of updated key should fail",
			expected:           peer.TxValidationCode_MVCC_READ_CONFLICT,
			waitForPropagation: true,
			rw: &kvrwset.KVRWSet{
				Reads: []*kvrwset.KVRead{
					{
						Key: key,
						Version: &kvrwset.Version{
							BlockNum: height,
							TxNum:    0,
						},
					},
				},
				Writes: []*kvrwset.KVWrite{
					{Key: key + "no", Value: []byte(`should fail`)},
				},
			},
		},
		{
			name:     "delete key",
			expected: peer.TxValidationCode_VALID,
			rw: &kvrwset.KVRWSet{
				Writes: []*kvrwset.KVWrite{
					{Key: key + "new", IsDelete: true},
				},
			},
		},
	}

	ids := []string{}
	for _, tc := range tests {
		t.Run("submit_"+tc.name, func(t *testing.T) {
			if tc.waitForPropagation {
				time.Sleep(2500 * time.Millisecond)
			}
			id := submit(t, c, tc.rw)
			ids = append(ids, id)
		})

	}
	// wait until last transaction is propagated and processed, and validate their status
	time.Sleep(2500 * time.Millisecond)
	for i, tc := range tests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			validate(t, c, ids[i], tc.expected)
		})
	}

	// check database
	checkValue(t, c.Storage, key, []byte(lastVal), false)
	checkValue(t, c.Storage, key+"new", nil, true)

	checkHistory(t, c.Storage, key, 2)
	checkHistory(t, c.Storage, key+"new", 2)
	checkHistory(t, c.Storage, key+"no", 0)

	// Check final block height
	newHeight, err := c.Committer.PeerBlockHeight()
	if err != nil {
		t.Error(err)
	}
	if newComHeight, err := c.Committer.BlockHeight(); err != nil {
		t.Error(err)
	} else if newComHeight != newHeight {
		t.Errorf("committer height %d != peer height %d", newComHeight, newHeight)
	}
	t.Logf("final blockheight: %d", newHeight)
}

func submit(t *testing.T, c *Client, rw *kvrwset.KVRWSet) string {
	tx, id, err := fabrictx.NewEndorserTransaction(Channel, Chaincode, c.Submitter, c.Endorsers, rw)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Orderer.Broadcast(tx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func validate(t *testing.T, c *Client, id string, expectedState peer.TxValidationCode) {
	info, err := c.TransactionByID(Channel, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.ValidationCode != int32(expectedState) {
		t.Errorf("expected tx %s to be %s, got validation code %s", id, peer.TxValidationCode_name[int32(expectedState)], peer.TxValidationCode_name[info.ValidationCode])
	}
}

func checkHistory(t *testing.T, store *committer.Store, key string, expectedLen int) {
	history, err := store.GetHistory(Chaincode, key)
	if err != nil {
		t.Error(err)
	} else {
		if len(history) != expectedLen {
			t.Errorf("should have 2 historic records for %s (got %d): %+v", key, len(history), history)
		}
	}
}

func checkValue(t *testing.T, store *committer.Store, key string, expectedVal []byte, deleted bool) {
	k, err := store.GetCurrent(Chaincode, key)
	if err != nil {
		t.Error(err)
		return
	}
	if k == nil {
		t.Errorf("key not found: %s", key+"new")
		return
	}
	if deleted {
		if !k.IsDelete {
			t.Errorf("key should be deleted: %s", key+"new")
		}
		if len(k.Value) != 0 {
			t.Errorf("key should have empty value: %s", key+"new")
		}
		return
	}

	if !bytes.Equal(k.Value, expectedVal) {
		t.Errorf("key %s: %s != %s", key, string(k.Value), string(expectedVal))
	}
}

func getTransactionById(channelName, txID string) *peer.ChaincodeInvocationSpec {
	return &peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{
			Type: peer.ChaincodeSpec_GOLANG,
			ChaincodeId: &peer.ChaincodeID{
				Name: "qscc",
			},
			Input: &peer.ChaincodeInput{
				Args: [][]byte{
					[]byte("GetTransactionByID"),
					[]byte(channelName),
					[]byte(txID),
				},
			},
		},
	}
}
