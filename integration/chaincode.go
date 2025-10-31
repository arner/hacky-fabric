package integration

import (
	"crypto/x509"

	"github.com/arner/hacky-fabric/storage"
	"github.com/hyperledger/fabric-chaincode-go/v2/pkg/cid"
	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewChaincodeExecutor(namespace string, db *storage.VersionedDB) *ChaincodeExecutor {
	return &ChaincodeExecutor{
		namespace: namespace,
		readStore: db,
	}
}

type ChaincodeExecutor struct {
	namespace string
	readStore *storage.VersionedDB
}

func (e ChaincodeExecutor) NewTransaction() (*TransactionContext, error) {
	stub, err := e.readStore.NewSimulationStore(e.namespace, 0, false)
	if err != nil {
		return nil, err
	}
	return &TransactionContext{
		Stub:           &FabricStub{SimulationStore: stub},
		ClientIdentity: ClientIdentity{},
	}, nil
}

type TransactionContext struct {
	Stub           *FabricStub
	ClientIdentity cid.ClientIdentity
}

func (t TransactionContext) Rwset() *kvrwset.KVRWSet {
	return Rwset(t.Stub.Result())
}

func Rwset(res storage.ReadWriteSet) *kvrwset.KVRWSet {
	rws := &kvrwset.KVRWSet{
		Reads:  make([]*kvrwset.KVRead, len(res.Reads)),
		Writes: make([]*kvrwset.KVWrite, len(res.Writes)),
	}
	for i, r := range res.Reads {
		read := &kvrwset.KVRead{Key: r.Key}
		if r.Version != nil {
			read.Version = &kvrwset.Version{
				BlockNum: r.Version.BlockNum,
				TxNum:    uint64(r.Version.TxNum),
			}
		}
		rws.Reads[i] = read
	}
	for i, r := range res.Writes {
		rws.Writes[i] = &kvrwset.KVWrite{
			Key:      r.Key,
			IsDelete: r.IsDelete,
			Value:    r.Value,
		}
	}
	return rws
}

type FabricStub struct {
	storage.SimulationStore
	UnimplementedStub
}

// GetClientIdentity implements contractapi.TransactionContextInterface.
func (t TransactionContext) GetClientIdentity() cid.ClientIdentity {
	return t.ClientIdentity
}

// GetStub implements contractapi.TransactionContextInterface.
func (t TransactionContext) GetStub() shim.ChaincodeStubInterface {
	return t.Stub
}

// ClientIdentity would be extracted from the input EndorserTransaction.
// See: github.com/hyperledger/fabric-chaincode-go/shim/stub.go
type ClientIdentity struct{}

// AssertAttributeValue implements cid.ClientIdentity.
func (c ClientIdentity) AssertAttributeValue(attrName string, attrValue string) error {
	panic("unimplemented")
}

// GetAttributeValue implements cid.ClientIdentity.
func (c ClientIdentity) GetAttributeValue(attrName string) (value string, found bool, err error) {
	panic("unimplemented")
}

// GetID implements cid.ClientIdentity.
func (c ClientIdentity) GetID() (string, error) {
	panic("unimplemented")
}

// GetMSPID implements cid.ClientIdentity.
func (c ClientIdentity) GetMSPID() (string, error) {
	panic("unimplemented")
}

// GetX509Certificate implements cid.ClientIdentity.
func (c ClientIdentity) GetX509Certificate() (*x509.Certificate, error) {
	panic("unimplemented")
}

// UnimplementedStub has functions to fulfil shim.ChaincodeStubInterface.
// See: github.com/hyperledger/fabric-chaincode-go/shim/stub.go
type UnimplementedStub struct{}

// ------------- Call Chaincode functions ---------------

// InvokeChaincode implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) InvokeChaincode(chaincodeName string, args [][]byte, channel string) *peer.Response {
	panic("unimplemented")
}

// ------------- Transaction metadata -------------

// GetFunctionAndParameters implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetFunctionAndParameters() (string, []string) {
	panic("unimplemented")
}

// GetArgs implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetArgs() [][]byte {
	panic("unimplemented")
}

// GetArgsSlice implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetArgsSlice() ([]byte, error) {
	panic("unimplemented")
}

// GetStringArgs implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStringArgs() []string {
	panic("unimplemented")
}

// GetTransient implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetTransient() (map[string][]byte, error) {
	panic("unimplemented")
}

// GetTxID implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetTxID() string {
	panic("unimplemented")
}

// GetTxTimestamp implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetTxTimestamp() (*timestamppb.Timestamp, error) {
	panic("unimplemented")
}

// GetBinding implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetBinding() ([]byte, error) {
	panic("unimplemented")
}

// GetChannelID implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetChannelID() string {
	panic("unimplemented")
}

// GetCreator implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetCreator() ([]byte, error) {
	panic("unimplemented")
}

// GetDecorations implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetDecorations() map[string][]byte {
	panic("unimplemented")
}

// GetSignedProposal implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetSignedProposal() (*peer.SignedProposal, error) {
	panic("unimplemented")
}

// --------- State functions ----------

// Basic state (fairly easy to add)

// GetHistoryForKey implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetHistoryForKey(key string) (shim.HistoryQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetMultipleStates implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetMultipleStates(keys ...string) ([][]byte, error) {
	panic("unimplemented")
}

// GetStateByRange implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStateByRange(startKey string, endKey string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetStateByRangeWithPagination implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStateByRangeWithPagination(startKey string, endKey string, pageSize int32, bookmark string) (shim.StateQueryIteratorInterface, *peer.QueryResponseMetadata, error) {
	panic("unimplemented")
}

// Events

// SetEvent implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) SetEvent(name string, payload []byte) error {
	panic("unimplemented")
}

// Rich queries (db specific)

// GetQueryResult implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetQueryResult(query string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetQueryResultWithPagination implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetQueryResultWithPagination(query string, pageSize int32, bookmark string) (shim.StateQueryIteratorInterface, *peer.QueryResponseMetadata, error) {
	panic("unimplemented")
}

// Composite keys
// (!) Fabric uses non-utf8 characters which is not supported by Postgres.

// CreateCompositeKey implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) CreateCompositeKey(objectType string, attributes []string) (string, error) {
	return shim.CreateCompositeKey(objectType, attributes)
}

// SplitCompositeKey source: github.com/hyperledger/fabric-chaincode-go/shim/shim.go
func (s UnimplementedStub) SplitCompositeKey(compositeKey string) (string, []string, error) {
	componentIndex := 1
	components := []string{}
	for i := 1; i < len(compositeKey); i++ {
		if compositeKey[i] == 0 { // U+0000
			components = append(components, compositeKey[componentIndex:i])
			componentIndex = i + 1
		}
	}
	return components[0], components[1:], nil
}

// GetStateByPartialCompositeKey implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStateByPartialCompositeKey(objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetStateByPartialCompositeKeyWithPagination implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStateByPartialCompositeKeyWithPagination(objectType string, keys []string, pageSize int32, bookmark string) (shim.StateQueryIteratorInterface, *peer.QueryResponseMetadata, error) {
	panic("unimplemented")
}

// GetAllStatesCompositeKeyWithPagination implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetAllStatesCompositeKeyWithPagination(pageSize int32, bookmark string) (shim.StateQueryIteratorInterface, *peer.QueryResponseMetadata, error) {
	panic("unimplemented")
}

// Key based endorsement

// SetStateValidationParameter implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) SetStateValidationParameter(key string, ep []byte) error {
	panic("unimplemented")
}

// GetStateValidationParameter implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetStateValidationParameter(key string) ([]byte, error) {
	panic("unimplemented")
}

// Fabric 3 batches

// StartWriteBatch implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) StartWriteBatch() {
	panic("unimplemented")
}

// FinishWriteBatch implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) FinishWriteBatch() error {
	panic("unimplemented")
}

// --------- Private data ----------

// GetPrivateData implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateData(collection string, key string) ([]byte, error) {
	panic("unimplemented")
}

// PutPrivateData implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) PutPrivateData(collection string, key string, value []byte) error {
	panic("unimplemented")
}

// DelPrivateData implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) DelPrivateData(collection string, key string) error {
	panic("unimplemented")
}

// PurgePrivateData implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) PurgePrivateData(collection string, key string) error {
	panic("unimplemented")
}

// GetMultiplePrivateData implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetMultiplePrivateData(collection string, keys ...string) ([][]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataByRange implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateDataByRange(collection string, startKey string, endKey string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetPrivateDataByPartialCompositeKey implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateDataByPartialCompositeKey(collection string, objectType string, keys []string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetPrivateDataQueryResult implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateDataQueryResult(collection string, query string) (shim.StateQueryIteratorInterface, error) {
	panic("unimplemented")
}

// GetPrivateDataHash implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateDataHash(collection string, key string) ([]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataValidationParameter implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) GetPrivateDataValidationParameter(collection string, key string) ([]byte, error) {
	panic("unimplemented")
}

// SetPrivateDataValidationParameter implements shim.ChaincodeStubInterface.
func (s UnimplementedStub) SetPrivateDataValidationParameter(collection string, key string, ep []byte) error {
	panic("unimplemented")
}
