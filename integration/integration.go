package integration

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path"

	"github.com/arner/hacky-fabric/comm"
	"github.com/arner/hacky-fabric/committer"
	"github.com/arner/hacky-fabric/fabrictx"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

type Client struct {
	Peer      *comm.Peer
	Orderer   *comm.Orderer
	Committer *committer.Committer
	Storage   *committer.Store
	Submitter fabrictx.Signer
	Endorsers []fabrictx.Signer
}

func NewClient(ctx context.Context, dir, dbConn string, logger committer.Logger) (*Client, error) {
	// peer
	peerTLS, err := os.ReadFile(path.Join(dir, "peer-tls-ca.crt"))
	if err != nil {
		return nil, err
	}
	peer, err := comm.NewPeer("peer0.org1.example.com:7051", peerTLS)
	if err != nil {
		return nil, err
	}

	// orderer
	pem, err := os.ReadFile(path.Join(dir, "orderer-tls-ca.crt"))
	if err != nil {
		return nil, err
	}
	orderer, err := comm.NewOrderer("orderer.example.com:7050", pem)
	if err != nil {
		return nil, err
	}

	// identities: submitter and two endorsers
	submitter, err := fabrictx.SignerFromMSP(path.Join(dir, "user"), "Org1MSP")
	if err != nil {
		return nil, err
	}

	endorser, err := fabrictx.SignerFromMSP(path.Join(dir, "endorser"), "Org1MSP")
	if err != nil {
		return nil, err
	}

	endorser2, err := fabrictx.SignerFromMSP(path.Join(dir, "endorser2"), "Org2MSP")
	if err != nil {
		return nil, err
	}

	// committer
	db, _ := sql.Open("sqlite", dbConn)
	store := committer.NewStorage("mychannel", db, "sqlite")
	err = store.Init()
	if err != nil {
		return nil, err
	}

	committer, err := committer.NewCommitter(ctx, store, "mychannel", peer, submitter, logger)
	if err != nil {
		return nil, err
	}

	return &Client{
		Peer:      peer,
		Orderer:   orderer,
		Storage:   store,
		Committer: committer,
		Submitter: submitter,
		Endorsers: []fabrictx.Signer{endorser, endorser2},
	}, nil
}

func (c Client) TransactionByID(channel, id string) (*peer.ProcessedTransaction, error) {
	prop, err := fabrictx.NewProposal(c.Submitter, channel, "qscc", [][]byte{[]byte("GetTransactionByID"), []byte(channel), []byte(id)})
	if err != nil {
		return nil, err
	}
	res, err := c.Peer.ProcessProposal(prop)
	if err != nil {
		return nil, err
	}
	ptx := &peer.ProcessedTransaction{}
	if err := proto.Unmarshal(res.Response.Payload, ptx); err != nil {
		return nil, err
	}
	return ptx, nil
}

func (c Client) Close() error {
	c.Committer.Stop()

	return errors.Join(
		c.Orderer.Close(),
		c.Peer.Close(),
	)
}
