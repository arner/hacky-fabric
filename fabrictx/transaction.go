package fabrictx

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewEndorserTransaction creates a new transaction envelope with all the necessary signatures and the provided read/write set.
func NewEndorserTransaction(channel, chaincode string, submitter Signer, endorsers []Signer, rwSet *kvrwset.KVRWSet) (*common.Envelope, string, error) {
	nsRwset := []*rwset.NsReadWriteSet{
		{
			Namespace: chaincode,
			Rwset:     mustMarshal(rwSet),
		},
	}
	return NewEndorserTxWithNsRwSet(channel, chaincode, "1.0", submitter, endorsers, nsRwset)
}

func NewEndorserTxWithNsRwSet(channel, chaincode, version string, submitter Signer, endorsers []Signer, nsRWSet []*rwset.NsReadWriteSet) (*common.Envelope, string, error) {
	// headers
	ccID := &peer.ChaincodeID{Name: chaincode, Version: version}
	creator, err := submitter.Serialize()
	if err != nil {
		return nil, "", err
	}
	hdr, txID := header(channel, creator, ccID, common.HeaderType_ENDORSER_TRANSACTION)

	// proposal payload
	chaincodeProposalPayload := mustMarshal(&peer.ChaincodeProposalPayload{
		Input: mustMarshal(&peer.ChaincodeInvocationSpec{
			ChaincodeSpec: &peer.ChaincodeSpec{
				Type:        peer.ChaincodeSpec_GOLANG,
				ChaincodeId: ccID,
				Input: &peer.ChaincodeInput{
					Args: [][]byte{[]byte("function_name")}, // not used CommitChaincodeDefinition basic1.0
				},
			},
		}),
	})
	pHash, err := getProposalHash(hdr, chaincodeProposalPayload)
	if err != nil {
		return nil, "", err
	}

	// proposal response payload
	proposalResponsePayload := mustMarshal(&peer.ProposalResponsePayload{
		ProposalHash: pHash,
		Extension: mustMarshal(&peer.ChaincodeAction{
			ChaincodeId: ccID,
			Results: mustMarshal(&rwset.TxReadWriteSet{
				NsRwset: nsRWSet,
			}),
			Events:   []byte{}, // empty
			Response: &peer.Response{Status: 200, Message: "OK"},
		}),
	})

	// endorsements
	endorsements := make([]*peer.Endorsement, len(endorsers))
	for i, signer := range endorsers {
		e, err := endorse(proposalResponsePayload, signer)
		if err != nil {
			return nil, "", err
		}
		endorsements[i] = e
	}

	// payload
	payload := &common.Payload{
		Header: hdr,
		Data: mustMarshal(&peer.Transaction{
			Actions: []*peer.TransactionAction{
				{
					Header: hdr.SignatureHeader,
					Payload: mustMarshal(&peer.ChaincodeActionPayload{
						ChaincodeProposalPayload: chaincodeProposalPayload,
						Action: &peer.ChaincodeEndorsedAction{
							ProposalResponsePayload: proposalResponsePayload,
							Endorsements:            endorsements,
						},
					}),
				},
			},
		}),
	}

	pl := mustMarshal(payload)
	sig, err := submitter.Sign(pl)
	if err != nil {
		return nil, "", err
	}

	return &common.Envelope{
		Payload:   pl,
		Signature: sig,
	}, txID, nil
}

func header(channel string, creator []byte, ccID *peer.ChaincodeID, typ common.HeaderType) (*common.Header, string) {
	tm := timestamppb.Now()
	tm.Nanos = 0
	nonce := mustNonce()

	cHdr := &common.ChannelHeader{
		Type:      int32(typ),
		Version:   0,
		Timestamp: tm,
		ChannelId: channel,
		Epoch:     0,
	}

	// not required for all header types
	if ccID != nil {
		cHdr.Extension = mustMarshal(&peer.ChaincodeHeaderExtension{ChaincodeId: ccID})
		cHdr.TxId = computeTxID(nonce, creator)
	}

	channelHeader := mustMarshal(cHdr)

	return &common.Header{
		ChannelHeader:   channelHeader,
		SignatureHeader: mustMarshal(&common.SignatureHeader{Creator: creator, Nonce: nonce}),
	}, cHdr.TxId
}

func computeTxID(nonce, creator []byte) string {
	hasher := sha256.New()
	hasher.Write(nonce)
	hasher.Write(creator)
	return hex.EncodeToString(hasher.Sum(nil))
}

func getProposalHash(header *common.Header, ccPropPayl []byte) ([]byte, error) {
	hash := sha256.New()
	hash.Write(header.ChannelHeader)
	hash.Write(header.SignatureHeader)
	hash.Write(ccPropPayl)
	return hash.Sum(nil), nil
}

func endorse(payload []byte, signer Signer) (*peer.Endorsement, error) {
	ser, err := signer.Serialize()
	if err != nil {
		return nil, err
	}
	sig, err := signer.Sign(append(payload, ser...))
	if err != nil {
		return nil, err
	}

	return &peer.Endorsement{
		Endorser:  ser,
		Signature: sig,
	}, nil
}

func mustNonce() []byte {
	key := make([]byte, 24)
	_, err := rand.Read(key)
	if err != nil {
		panic(err)
	}
	return key
}

// mustMarshal is like protoutil.MarshalOrPanic but stays in apiv1.
func mustMarshal(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}
