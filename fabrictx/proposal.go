package fabrictx

import (
	"fmt"
	"math"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

// NewProposal creates a new proposal to be submitted to a peer
func NewProposal(submitter Signer, channel, chaincode string, args [][]byte) (*peer.SignedProposal, error) {
	// header
	signer, err := submitter.Serialize()
	if err != nil {
		return nil, err
	}

	h, _ := header(channel, signer, &peer.ChaincodeID{Name: chaincode}, common.HeaderType_ENDORSER_TRANSACTION)
	hdr, err := proto.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("marshal header: %w", err)
	}

	// payload
	invocation, err := proto.Marshal(&peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{
			Type: peer.ChaincodeSpec_GOLANG,
			ChaincodeId: &peer.ChaincodeID{
				Name: chaincode,
			},
			Input: &peer.ChaincodeInput{
				Args: args,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal ChaincodeInvocationSpec: %w", err)
	}
	payload, err := proto.Marshal(&peer.ChaincodeProposalPayload{Input: invocation})
	if err != nil {
		return nil, fmt.Errorf("marshal ChaincodeProposalPayload: %w", err)
	}

	// signed proposal
	proposal, err := proto.Marshal(&peer.Proposal{Header: hdr, Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("marshal proposal: %w", err)
	}
	sig, err := submitter.Sign(proposal)
	if err != nil {
		return nil, err
	}

	return &peer.SignedProposal{
		ProposalBytes: proposal,
		Signature:     sig,
	}, nil
}

// NewDeliverSeekInfo returns a signed envelope that can be used to subscribe to a peer
func NewDeliverSeekInfo(submitter Signer, channel string, startBlock uint64) (*common.Envelope, error) {
	signer, err := submitter.Serialize()
	if err != nil {
		return nil, err
	}
	hdr, _ := header(channel, signer, nil, common.HeaderType_DELIVER_SEEK_INFO)

	var start *orderer.SeekPosition
	if startBlock == 0 {
		start = &orderer.SeekPosition{Type: &orderer.SeekPosition_Newest{Newest: &orderer.SeekNewest{}}}
	} else {
		start = &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startBlock}}}
	}
	seekInfo := &orderer.SeekInfo{
		Start:    start,
		Stop:     &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY,
	}
	seekBytes, err := proto.Marshal(seekInfo)
	if err != nil {
		return nil, fmt.Errorf("marshal SeekInfo: %w", err)
	}

	payload, err := proto.Marshal(&common.Payload{
		Header: hdr,
		Data:   seekBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal Payload: %w", err)
	}

	sig, err := submitter.Sign(payload)
	if err != nil {
		return nil, fmt.Errorf("sign payload: %w", err)
	}

	return &common.Envelope{
		Payload:   payload,
		Signature: sig,
	}, nil
}
