package fabrictx_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/arner/hacky-fabric/fabrictx"
	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/msp/mgmt"
)

func TestCreateAndConvertEndorserTx(t *testing.T) {
	submitter, endorsers := getTestUsers(t)
	rw := &kvrwset.KVRWSet{
		Writes: []*kvrwset.KVWrite{
			{Key: "my_test_key", Value: []byte(`{"MyVal":"hello world"}`)},
		},
	}

	tx, id, err := fabrictx.NewEndorserTransaction("mychannel", "basic", submitter, endorsers, rw)
	if err != nil {
		t.Fatal(err)
	}
	if len(id) == 0 {
		t.Fatal("expected transactionID to be generated")
	}

	// validate general structure
	if err = validateEnvelope("fixtures/endorser", "Org1MSP", "mychannel", tx); err != nil {
		t.Fatal(err)
	}

	parsed, err := fabrictx.EndorserTxToStruct(tx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// validate endorsement
	err = validateEndorsementSignatures(parsed.Payload.Data.Actions[0], []string{"Org1MSP", "Org2MSP"})
	if err != nil {
		t.Fatal(err)
	}
}

// validateEnvelope verifies the submitter signature, internal consistency and format.
func validateEnvelope(submitterMSPDir, submitterMSPID, channel string, tx *common.Envelope) error {
	ks := sw.NewDummyKeyStore()
	cryptoP, _ := sw.New(ks)
	m, err := msp.NewBccspMspWithKeyStore(msp.MSPv3_0, ks, cryptoP)
	if err != nil {
		return err
	}

	mspConf, err := msp.GetVerifyingMspConfig(submitterMSPDir, submitterMSPID, msp.ProviderTypeToString(msp.FABRIC))
	if err != nil {
		return err
	}
	if err := m.Setup(mspConf); err != nil {
		return err
	}
	mgr := msp.NewMSPManager()
	err = mgr.Setup([]msp.MSP{m})
	if err != nil {
		return err
	}
	mgmt.XXXSetMSPManager(channel, mgr)

	pl, code := validation.ValidateTransaction(tx, cryptoP)
	if code != peer.TxValidationCode_VALID {
		return errors.New(string(code))
	}
	if pl == nil {
		return errors.New("payload should not be nil")
	}
	return nil
}

// validateEndorsementSignatures checks whether the signatures by the provide mspIDs are valid.
// it does not know whether the certificates are issued by the right CAs or what the policy is.
func validateEndorsementSignatures(act fabrictx.Action, mspIDs []string) error {
	ctr := 0
	for _, m := range mspIDs {
		for _, end := range act.Endorsements {
			if m != end.Endorser.Mspid {
				continue
			}
			if err := end.Verify(act.ProposalResponsePayloadB); err != nil {
				return nil
			}
			ctr++
		}

	}
	if ctr < len(mspIDs) {
		return fmt.Errorf("expected signatures from %v, got only %d", mspIDs, ctr)
	}

	return nil
}

func getTestUsers(t *testing.T) (fabrictx.Signer, []fabrictx.Signer) {
	submitter, err := fabrictx.SignerFromMSP("fixtures/user", "Org1MSP")
	if err != nil {
		t.Fatal(err)
	}
	endorser, err := fabrictx.SignerFromMSP("fixtures/endorser", "Org1MSP")
	if err != nil {
		t.Fatal(err)
	}
	endorser2, err := fabrictx.SignerFromMSP("fixtures/endorser2", "Org2MSP")
	if err != nil {
		t.Fatal(err)
	}
	return submitter, []fabrictx.Signer{endorser, endorser2}
}
