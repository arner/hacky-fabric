package integration

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"path"
	"testing"
	"time"

	"github.com/arner/hacky-fabric/storage"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	_ "modernc.org/sqlite"
)

const (
	Channel   = "mychannel"
	Namespace = "basic"
)

type testLogger struct {
	t *testing.T
}

func (tl testLogger) Printf(format string, v ...any) {
	tl.t.Logf(format, v...)
}

// TestClientTransactions requires Fabric to be running (see readme)
func TestClientTransactions(t *testing.T) {
	c := createAndStartClient(t)
	defer c.Close()

	height, err := c.Committer.PeerBlockHeight()
	if err != nil {
		t.Fatal(err)
	}

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
				time.Sleep(2200 * time.Millisecond)
			}
			id, err := c.EndorseAndSubmit(Channel, Namespace, tc.rw)
			if err != nil {
				t.Error(err)
			}
			ids = append(ids, id)
		})

	}
	// wait until last transaction is propagated and processed, and validate their status
	time.Sleep(2200 * time.Millisecond)
	for i, tc := range tests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			validate(t, c, ids[i], tc.expected)
		})
	}

	// check database
	checkValue(t, c.DB, key, []byte(lastVal), false)
	checkValue(t, c.DB, key+"new", nil, true)

	checkHistory(t, c.DB, key, 2)
	checkHistory(t, c.DB, key+"new", 2)
	checkHistory(t, c.DB, key+"no", 0)

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

func createAndStartClient(t *testing.T) *Client {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	store := storage.New("mychannel", db)
	err = store.Init()
	if err != nil {
		t.Fatal(err)
	}

	cwd, _ := os.Getwd()
	samplesDir := cmp.Or(os.Getenv("FABRIC_SAMPLES"), path.Join(cwd, "..", "fabric-samples"))
	c, err := NewClientForFabricSamples(t.Context(), samplesDir, store, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}

	// the committer processes blocks in the background
	go c.Committer.Run()

	// make sure the committer is synced before we start (block the thread)
	// timeout is too aggressive for a real chain.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	err = c.Committer.WaitUntilSynced(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	return c
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

func checkHistory(t *testing.T, db *storage.VersionedDB, key string, expectedLen int) {
	history, err := db.GetHistory(Namespace, key)
	if err != nil {
		t.Error(err)
	} else {
		if len(history) != expectedLen {
			t.Errorf("should have 2 historic records for %s (got %d): %+v", key, len(history), history)
		}
	}
}

func checkValue(t *testing.T, db *storage.VersionedDB, key string, expectedVal []byte, deleted bool) {
	k, err := db.GetCurrent(Namespace, key)
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
