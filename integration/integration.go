package integration

import (
	"context"
	"errors"
	"os"
	"path"

	"github.com/arner/hacky-fabric/comm"
	"github.com/arner/hacky-fabric/committer"
	"github.com/arner/hacky-fabric/fabrictx"
	"github.com/arner/hacky-fabric/storage"

	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	Peer      *comm.Peer
	Orderer   *comm.Orderer
	Committer *committer.Committer
	DB        *storage.VersionedDB
	Submitter fabrictx.Signer
	Endorsers []fabrictx.Signer
}

// NewClientForFabricSamples returns a client for integration testing with access to a peer, orderer and local committer.
// It follows the directory structure of a fabric samples test network.
func NewClientForFabricSamples(ctx context.Context, samplesDir string, db *storage.VersionedDB, logger committer.Logger) (*Client, error) {
	org1 := path.Join(samplesDir, "test-network", "organizations", "peerOrganizations", "org1.example.com")
	org2 := path.Join(samplesDir, "test-network", "organizations", "peerOrganizations", "org2.example.com")
	ordererOrg := path.Join(samplesDir, "test-network", "organizations", "ordererOrganizations", "example.com")

	// peer
	peerTLS, err := os.ReadFile(path.Join(org1, "tlsca", "tlsca.org1.example.com-cert.pem"))
	if err != nil {
		return nil, err
	}
	peer, err := comm.NewPeer("peer0.org1.example.com:7051", peerTLS)
	if err != nil {
		return nil, err
	}

	// orderer
	pem, err := os.ReadFile(path.Join(ordererOrg, "tlsca", "tlsca.example.com-cert.pem"))
	if err != nil {
		return nil, err
	}
	orderer, err := comm.NewOrderer("orderer.example.com:7050", pem)
	if err != nil {
		return nil, err
	}

	// identities: submitter and two endorsers
	submitter, err := fabrictx.SignerFromMSP(path.Join(org1, "users", "User1@org1.example.com", "msp"), "Org1MSP")
	if err != nil {
		return nil, err
	}
	endorser, err := fabrictx.SignerFromMSP(path.Join(org1, "peers", "peer0.org1.example.com", "msp"), "Org1MSP")
	if err != nil {
		return nil, err
	}
	endorser2, err := fabrictx.SignerFromMSP(path.Join(org2, "peers", "peer0.org2.example.com", "msp"), "Org2MSP")
	if err != nil {
		return nil, err
	}

	// committer
	committer, err := committer.NewCommitter(ctx, db, "mychannel", peer, submitter, logger)
	if err != nil {
		return nil, err
	}

	return &Client{
		Peer:      peer,
		Orderer:   orderer,
		DB:        db,
		Committer: committer,
		Submitter: submitter,
		Endorsers: []fabrictx.Signer{endorser, endorser2},
	}, nil
}

// TransactionByID retrieves a specific transaction from the peer.
func (c Client) TransactionByID(channel, id string) (*peer.ProcessedTransaction, error) {
	res, err := c.Query(channel, "qscc", "GetTransactionByID", [][]byte{[]byte(channel), []byte(id)})
	if err != nil {
		return nil, err
	}
	ptx := &peer.ProcessedTransaction{}
	if err := proto.Unmarshal(res.Response.Payload, ptx); err != nil {
		return nil, err
	}
	return ptx, nil
}

// Query sends a query request to the peer.
func (c Client) Query(channel, namespace, fn string, args [][]byte) (*peer.ProposalResponse, error) {
	prop, err := fabrictx.NewProposal(c.Submitter, channel, namespace, append([][]byte{[]byte(fn)}, args...))
	if err != nil {
		return nil, err
	}
	return c.Peer.ProcessProposal(prop)
}

// EndorseAndSubmit creates a transaction out of a read/write set and endorses it with the configured endorser keys.
func (c Client) EndorseAndSubmit(channel, namespace string, rw *kvrwset.KVRWSet) (string, error) {
	tx, id, err := fabrictx.NewEndorserTransaction(channel, namespace, c.Submitter, c.Endorsers, rw)
	if err != nil {
		return "", err
	}
	err = c.Orderer.Broadcast(tx)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c Client) Close() error {
	c.Committer.Stop()

	return errors.Join(
		c.Orderer.Close(),
		c.Peer.Close(),
	)
}
