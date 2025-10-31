package comm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/arner/hacky-fabric/fabrictx"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Peer struct {
	conn   *grpc.ClientConn
	client peer.EndorserClient
	ctx    context.Context
	cancel context.CancelFunc
}

// StorageProvider defines how to persist committed writes
type StorageProvider interface {
	StoreWrite(namespace string, block uint64, tx int, key string, value []byte) error
}

func NewPeer(addr string, tlsPem []byte) (*Peer, error) {
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(tlsPem); !ok {
		return nil, fmt.Errorf("failed to append peer TLS cert")
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("peer address [%s] must contain port: %w", addr, err)
	}
	creds := credentials.NewTLS(&tls.Config{
		RootCAs:    roots,
		ServerName: host,
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial peer: %w", err)
	}

	p := &Peer{
		conn:   conn,
		client: peer.NewEndorserClient(conn),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p, nil
}

// ProcessProposal takes a signed proposal and sends it to the peer.
func (p *Peer) ProcessProposal(prop *peer.SignedProposal) (*peer.ProposalResponse, error) {
	resp, err := p.client.ProcessProposal(p.ctx, prop)
	if err != nil {
		return nil, fmt.Errorf("process proposal: %w", err)
	}
	// Check response status
	if resp.Response.Status != 200 {
		return nil, fmt.Errorf("invocation failed with status %d: %s", resp.Response.Status, resp.Response.Message)
	}

	return resp, nil
}

// BlockHandler processes a single block with optional private data.
// Returning an error will stop the subscription.
type BlockHandler func(block *peer.DeliverResponse_BlockAndPrivateData) error

// SubscribeBlocks connects to the peer DeliverWithPrivateData service and streams blocks
// from the given starting block number, invoking the provided handler for each block.
func (p *Peer) SubscribeBlocks(channel string, startBlock uint64, signer fabrictx.Signer, handle BlockHandler) error {
	deliverClient := peer.NewDeliverClient(p.conn)

	deliver, err := deliverClient.DeliverWithPrivateData(p.ctx)
	if err != nil {
		return fmt.Errorf("open DeliverWithPrivateData: %w", err)
	}
	defer deliver.CloseSend()

	env, err := fabrictx.NewDeliverSeekInfo(signer, channel, startBlock)
	if err != nil {
		return fmt.Errorf("build seek envelope: %w", err)
	}
	if err := deliver.Send(env); err != nil {
		return fmt.Errorf("send seek envelope: %w", err)
	}

	for {
		msg, err := deliver.Recv()
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Canceled {
				// Peer connection is closing from our side.
				return nil
			} else {
				return fmt.Errorf("recv deliver: %w", err)
			}
		}

		switch t := msg.Type.(type) {
		case *peer.DeliverResponse_BlockAndPrivateData:
			if err := handle(t); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		case *peer.DeliverResponse_Status:
			if t.Status != common.Status_SUCCESS {
				return fmt.Errorf("deliver stream ended: %s", t.Status)
			}
			return nil
		}
	}
}

func (p *Peer) Close() error {
	p.cancel()
	return p.conn.Close()
}
