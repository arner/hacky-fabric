package integration

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/hyperledger/fabric-samples/asset-transfer-basic/chaincode-go/chaincode"
	_ "modernc.org/sqlite"
)

func TestChaincode(t *testing.T) {
	c := createAndStartClient(t)
	defer c.Close()

	executor := NewChaincodeExecutor(Namespace, c.DB)

	// tx: init
	txc := newTx(t, executor)
	cc := &chaincode.SmartContract{}
	err := cc.InitLedger(txc)
	if err != nil {
		t.Fatal(err)
	}
	rws := txc.Rwset()
	_, err = c.EndorseAndSubmit(Channel, Namespace, rws)
	if err != nil {
		t.Error(err)
	}

	expected := &chaincode.Asset{
		ID:             rand.Text(),
		Color:          "red",
		Size:           3,
		Owner:          "me",
		AppraisedValue: 1000,
	}
	// tx: create asset
	txc = newTx(t, executor)
	err = cc.CreateAsset(txc, expected.ID, expected.Color, expected.Size, expected.Owner, expected.AppraisedValue)
	if err != nil {
		t.Fatal(err)
	}
	rws = txc.Rwset()
	_, err = c.EndorseAndSubmit(Channel, Namespace, rws)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(2200 * time.Millisecond) // wait till committed

	// tx: read asset
	txc = newTx(t, executor)
	asset, err := cc.ReadAsset(txc, expected.ID)
	if err != nil {
		t.Fatal(err)
	}
	if *asset != *expected {
		t.Errorf("%+v != %+v", asset, expected)
	}

	// Validate that the actual chaincode returns the same asset.
	a, err := c.Query("mychannel", "basic", "ReadAsset", [][]byte{[]byte(expected.ID)})
	if err != nil {
		t.Fatal(err)
	}
	asset = &chaincode.Asset{}
	err = json.Unmarshal(a.Response.Payload, asset)
	if *asset != *expected {
		t.Errorf("%+v != %+v (json err: %s)", asset, expected, err)
	}

	// TODO: range query
}

func newTx(t *testing.T, ex *ChaincodeExecutor) *TransactionContext {
	txc, err := ex.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}
	return txc
}

// local commit without any MVCC validations. We skip: core/ledger/kvledger/txmgmt/validation/validator.go
// func commit(store *committer.Store, namespace string, blockNum uint64, txID string, txNum int, rws *kvrwset.KVRWSet) error {
// 	writes := committer.Records(namespace, blockNum, txID, txNum, rws)
// 	if len(writes) > 0 {
// 		return store.BatchInsert(writes)
// 	}
// 	return nil
// }
