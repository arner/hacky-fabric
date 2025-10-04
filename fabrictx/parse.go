package fabrictx

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

type Envelope struct {
	Payload   Payload `json:"payload"`
	Signature []byte  `json:"signature"`
}

func (e Envelope) String() string {
	b, _ := json.MarshalIndent(e, "", "  ")
	return string(b)
}

type Header struct {
	ChannelHeader   *common.ChannelHeader   `json:"channel_header"`
	SignatureHeader *common.SignatureHeader `json:"signature_header"`
}

type Payload struct {
	Header Header `json:"header"`
	Data   Data   `json:"data"`
}

type Data struct {
	Actions []Action `json:"actions"`
}

type Action struct {
	ChaincodeProposalPayload ChaincodeProposalPayload `json:"chaincode_proposal_payload"`
	Endorsements             []Endorsement            `json:"endorsements"`
	ProposalResponsePayload  ProposalResponsePayload  `json:"proposal_response_payload"`
	ProposalResponsePayloadB []byte                   `json:"-"`
}

type Endorsement struct {
	Endorser  *msp.SerializedIdentity `json:"endorser"`
	EndorserB []byte                  `json:"-"`
	Signature []byte                  `json:"signature"`
}

// Verify verifies the validity of the signature over the payload. It does not know whether the endorser is part of the policy or whether the policy has been met.
func (e Endorsement) Verify(proposalResponsePayload []byte) error {
	// The message to be verified is the concatenation of the ProposalResponsePayload and the serialized endorser identity
	if err := VerifySignature(e.Endorser.IdBytes, e.Signature, append(proposalResponsePayload, e.EndorserB...)); err != nil {
		return fmt.Errorf("endorsement of %s invalid: %w", e.Endorser.Mspid, err)
	}
	return nil
}

type ChaincodeProposalPayload struct {
	Input *peer.ChaincodeInvocationSpec `json:"input"`
}

type ProposalResponsePayload struct {
	ProposalHash []byte    `json:"proposal_hash"`
	Extension    Extension `json:"extension"`
}

type Extension struct {
	ChaincodeID *peer.ChaincodeID    `json:"chaincode_id"`
	Response    *peer.Response       `json:"response"`
	Events      *peer.ChaincodeEvent `json:"events"`
	Results     []NsRwset            `json:"results"`
}

type NsRwset struct {
	Namespace string           `json:"namespace"`
	Rwset     *kvrwset.KVRWSet `json:"rwset"`
	TxID      string           `json:"-"`
}

func EndorserTxToStruct(env *common.Envelope) (Envelope, error) {
	out := Envelope{}
	hdr, tx, err := parseHeader(env)
	if err != nil {
		return out, err
	}

	// HeaderType_ENDORSER_TRANSACTION
	var actions []Action
	for _, act := range tx.Actions {
		action, err := parseAction(act)
		if err != nil {
			return Envelope{}, err
		}
		actions = append(actions, action)
	}

	return Envelope{
		Payload: Payload{
			Header: hdr,
			Data: Data{
				Actions: actions,
			},
		},
		Signature: env.Signature,
	}, nil
}

func parseHeader(env *common.Envelope) (Header, *peer.Transaction, error) {
	h := Header{}
	pl := &common.Payload{}
	if err := proto.Unmarshal(env.Payload, pl); err != nil {
		return h, nil, fmt.Errorf("payload: %w", err)
	}

	chdr := &common.ChannelHeader{}
	if err := proto.Unmarshal(pl.Header.ChannelHeader, chdr); err != nil {
		return h, nil, fmt.Errorf("channel header: %w", err)
	}

	shdr := &common.SignatureHeader{}
	if err := proto.Unmarshal(pl.Header.SignatureHeader, shdr); err != nil {
		return h, nil, fmt.Errorf("signature header: %w", err)
	}

	tx := &peer.Transaction{}
	if err := proto.Unmarshal(pl.Data, tx); err != nil {
		return h, nil, fmt.Errorf("transaction: %w", err)
	}
	h.ChannelHeader = chdr
	h.SignatureHeader = shdr

	return h, tx, nil
}

func parseAction(act *peer.TransactionAction) (Action, error) {
	a := Action{}
	cap := &peer.ChaincodeActionPayload{}
	if err := proto.Unmarshal(act.Payload, cap); err != nil {
		return a, fmt.Errorf("chaincode action payload: %w", err)
	}

	cpp := &peer.ChaincodeProposalPayload{}
	if err := proto.Unmarshal(cap.ChaincodeProposalPayload, cpp); err != nil {
		return a, fmt.Errorf("chaincode proposal payload: %w", err)
	}

	cis := &peer.ChaincodeInvocationSpec{}
	if err := proto.Unmarshal(cpp.Input, cis); err != nil {
		return a, fmt.Errorf("chaincode invocation spec: %w", err)
	}

	prp := &peer.ProposalResponsePayload{}
	if err := proto.Unmarshal(cap.Action.ProposalResponsePayload, prp); err != nil {
		return a, fmt.Errorf("proposal response payload: %w", err)
	}

	ccAct := &peer.ChaincodeAction{}
	if err := proto.Unmarshal(prp.Extension, ccAct); err != nil {
		return a, fmt.Errorf("chaincode action: %w", err)
	}

	txRWSet := &rwset.TxReadWriteSet{}
	if err := proto.Unmarshal(ccAct.Results, txRWSet); err != nil {
		return a, fmt.Errorf("rwset: %w", err)
	}

	events := &peer.ChaincodeEvent{}
	if err := proto.Unmarshal(ccAct.Events, events); err != nil {
		return a, fmt.Errorf("events: %w", err)
	}

	var nsList []NsRwset
	for _, ns := range txRWSet.NsRwset {
		kvs := &kvrwset.KVRWSet{}
		if err := proto.Unmarshal(ns.Rwset, kvs); err != nil {
			return a, fmt.Errorf("kvrwset: %w", err)
		}
		nsList = append(nsList, NsRwset{
			Namespace: ns.Namespace,
			Rwset:     kvs,
		})
	}

	endorsements := []Endorsement{}
	for _, end := range cap.Action.Endorsements {
		id := &msp.SerializedIdentity{}
		if err := proto.Unmarshal(end.Endorser, id); err != nil {
			return a, fmt.Errorf("endorser identity: %w", err)
		}
		endorsements = append(endorsements, Endorsement{
			Endorser:  id,
			EndorserB: end.Endorser,
			Signature: end.Signature,
		})
	}

	return Action{
		ChaincodeProposalPayload: ChaincodeProposalPayload{
			Input: cis,
		},
		Endorsements:             endorsements,
		ProposalResponsePayloadB: cap.Action.ProposalResponsePayload,
		ProposalResponsePayload: ProposalResponsePayload{
			ProposalHash: prp.ProposalHash,
			Extension: Extension{
				ChaincodeID: ccAct.ChaincodeId,
				Response:    ccAct.Response,
				Events:      events,
				Results:     nsList,
			},
		},
	}, nil
}

// RWSets retrieves the resulting reads and writes from a transaction.
func RWSets(env *common.Envelope) ([]NsRwset, error) {
	out := []NsRwset{}
	pl := &common.Payload{}
	if err := proto.Unmarshal(env.Payload, pl); err != nil {
		return out, fmt.Errorf("payload: %w", err)
	}
	chdr := &common.ChannelHeader{}
	if err := proto.Unmarshal(pl.Header.ChannelHeader, chdr); err != nil {
		return out, fmt.Errorf("channel header: %w", err)
	}
	txID := chdr.TxId

	tx := &peer.Transaction{}
	if err := proto.Unmarshal(pl.Data, tx); err != nil {
		return out, fmt.Errorf("transaction: %w", err)
	}

	for _, act := range tx.Actions {
		cap := &peer.ChaincodeActionPayload{}
		if err := proto.Unmarshal(act.Payload, cap); err != nil {
			return out, fmt.Errorf("chaincode action payload: %w", err)
		}
		prp := &peer.ProposalResponsePayload{}
		if err := proto.Unmarshal(cap.Action.ProposalResponsePayload, prp); err != nil {
			return out, fmt.Errorf("proposal response payload: %w", err)
		}

		ccAct := &peer.ChaincodeAction{}
		if err := proto.Unmarshal(prp.Extension, ccAct); err != nil {
			return out, fmt.Errorf("chaincode action: %w", err)
		}

		txRWSet := &rwset.TxReadWriteSet{}
		if err := proto.Unmarshal(ccAct.Results, txRWSet); err != nil {
			return out, fmt.Errorf("rwset: %w", err)
		}

		for _, ns := range txRWSet.NsRwset {
			kvs := &kvrwset.KVRWSet{}
			if err := proto.Unmarshal(ns.Rwset, kvs); err != nil {
				return out, fmt.Errorf("kvrwset: %w", err)
			}
			out = append(out, NsRwset{
				Namespace: ns.Namespace,
				Rwset:     kvs,
				TxID:      txID,
			})
		}
	}
	return out, nil
}
