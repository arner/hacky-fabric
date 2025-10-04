package comm

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Orderer struct {
	conn   *grpc.ClientConn
	client orderer.AtomicBroadcastClient
	stream orderer.AtomicBroadcast_BroadcastClient
	ctx    context.Context
	cancel context.CancelFunc
}

func NewOrderer(addr string, tlsPem []byte) (*Orderer, error) {
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(tlsPem); !ok {
		return nil, fmt.Errorf("failed to append orderer TLS cert")
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("orderer address [%s] must contain port: %w", addr, err)
	}
	creds := credentials.NewTLS(&tls.Config{
		RootCAs:    roots,
		ServerName: host,
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial orderer: %w", err)
	}
	o := &Orderer{
		conn:   conn,
		client: orderer.NewAtomicBroadcastClient(conn),
	}
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.stream, err = o.client.Broadcast(o.ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return o, nil
}

// Broadcast sends a signed envelope with an endorsed EndorserTransaction for ordering.
func (o Orderer) Broadcast(env *common.Envelope) error {
	if err := o.stream.Send(env); err != nil {
		return err
	}
	resp, err := o.stream.Recv()
	if err != nil {
		return err
	}
	if resp.Status != common.Status_SUCCESS {
		return fmt.Errorf("orderer rejected: %s", resp.Status.String())
	}
	return nil
}

func (o *Orderer) Close() error {
	if err := o.stream.CloseSend(); err != nil {
		o.cancel()
		_ = o.conn.Close()
		return err
	}
	o.cancel()
	return o.conn.Close()
}
