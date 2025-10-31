package committer

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/arner/hacky-fabric/comm"
	"github.com/arner/hacky-fabric/fabrictx"
	"github.com/arner/hacky-fabric/storage"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

type Logger interface {
	Printf(format string, v ...any)
}

type Committer struct {
	db      *storage.VersionedDB
	peer    *comm.Peer
	channel string
	signer  fabrictx.Signer
	ctx     context.Context
	cancel  context.CancelFunc
	log     Logger
}

func NewCommitter(ctx context.Context, db *storage.VersionedDB, channel string, peer *comm.Peer, signer fabrictx.Signer, logger Logger) (*Committer, error) {
	cctx, cancel := context.WithCancel(ctx)

	return &Committer{
		db:      db,
		peer:    peer,
		signer:  signer,
		channel: channel,
		ctx:     cctx,
		cancel:  cancel,
		log:     logger,
	}, nil
}

func (c *Committer) Run() error {
	lastBlock, _ := c.db.LastProcessedBlock()
	start := lastBlock + 1

	backoff := time.Second
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		err := c.peer.SubscribeBlocks(c.channel, start, c.signer, func(block *peer.DeliverResponse_BlockAndPrivateData) error {
			select {
			case <-c.ctx.Done():
				return fmt.Errorf("stopped")
			default:
			}
			return c.processBlock(block)
		})
		if err != nil {
			select {
			case <-c.ctx.Done():
				return nil
			default:
			}
			c.log.Printf("deliver error: %v — retrying in %s", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		backoff = time.Second
	}
}

// storeAsFile stores blocks as files
func storeAsFile(block *peer.DeliverResponse_BlockAndPrivateData) error {
	b := block.BlockAndPrivateData.Block
	blockB, err := proto.Marshal(b)
	if err != nil {
		return err
	}
	if err = os.WriteFile(fmt.Sprintf("./%d.block", b.Header.Number), blockB, 0644); err != nil {
		return err
	}

	return nil
}

func (c *Committer) processBlock(block *peer.DeliverResponse_BlockAndPrivateData) error {
	w, num, err := parseBlock(block, c.log)
	if err != nil {
		c.log.Printf("error parsing block: %s", err.Error()) // TODO error handling
	}
	// c.log.Printf("block %d - %d writes\n", num, len(w))
	if len(w) == 0 {
		if err := c.db.MarkProcessed(nil, num); err != nil {
			log.Printf("error marking block as processed: %s (ignoring)", err.Error()) // this breaks waitUntilSynced
		}
		return nil
	}
	return c.db.BatchInsert(w)
}

func parseBlock(block *peer.DeliverResponse_BlockAndPrivateData, log Logger) ([]storage.WriteRecord, uint64, error) {
	writes := []storage.WriteRecord{}

	b := block.BlockAndPrivateData.Block
	if len(b.Metadata.Metadata) <= int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		return writes, 0, fmt.Errorf("block metadata missing TRANSACTIONS_FILTER")
	}
	txFilter := b.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]

	for txNum, envBytes := range b.Data.Data {
		if txNum >= len(txFilter) || peer.TxValidationCode(txFilter[txNum]) != peer.TxValidationCode_VALID {
			log.Printf("%d:%d %s", b.Header.Number, txNum, peer.TxValidationCode_name[int32(peer.TxValidationCode(txFilter[txNum]))])
			continue
		}
		log.Printf("%d:%d %s", b.Header.Number, txNum, peer.TxValidationCode_name[int32(peer.TxValidationCode(txFilter[txNum]))])
		env := &common.Envelope{}
		if err := proto.Unmarshal(envBytes, env); err != nil {
			log.Printf("%d:%d invalid envelope: %s", b.Header.Number, txNum, err.Error())
			continue
		}
		rwsets, err := fabrictx.RWSets(env)
		if err != nil {
			log.Printf("%d:%d invalid rwset: %s", b.Header.Number, txNum, err.Error())
			continue
		}
		for _, rw := range rwsets {
			writes = append(writes, records(rw.Namespace, b.Header.Number, uint64(txNum), rw.TxID, rw.Rwset)...)
		}
	}
	return writes, b.Header.Number, nil
}

// records returns the writes in a format that makes them easy to store.
func records(namespace string, blockNum, txNum uint64, txID string, rws *kvrwset.KVRWSet) []storage.WriteRecord {
	writes := make([]storage.WriteRecord, len(rws.Writes))
	for i, w := range rws.Writes {
		writes[i] = storage.WriteRecord{
			Namespace: namespace,
			BlockNum:  blockNum,
			TxNum:     txNum,
			TxID:      txID,
			Key:       w.Key,
			Value:     w.Value,
			IsDelete:  w.IsDelete,
		}
	}
	return writes
}

func (c *Committer) BlockHeight() (uint64, error) {
	lpb, err := c.db.LastProcessedBlock()
	if err != nil {
		return 0, err
	}
	return lpb + 1, nil
}

func (c *Committer) PeerBlockHeight() (uint64, error) {
	prop, err := fabrictx.NewProposal(c.signer, c.channel, "qscc", [][]byte{[]byte("GetChainInfo"), []byte(c.channel)})
	if err != nil {
		return 0, err
	}
	res, err := c.peer.ProcessProposal(prop)
	if err != nil {
		return 0, err
	}

	info := &common.BlockchainInfo{}
	err = proto.Unmarshal(res.Response.Payload, info)
	if err != nil {
		return 0, err
	}

	return info.Height, nil
}

// WaitUntilSynced blocks until the committer has processed all blocks up to the peer's current height.
// Returns an error if the context is canceled or times out.
func (c *Committer) WaitUntilSynced(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("time out waiting for sync")
		case <-ticker.C:
			peerHeight, err := c.PeerBlockHeight()
			if err != nil {
				backoff *= 2
				c.log.Printf("error getting block height from peer: %s\n — retrying in %s", err.Error(), backoff)
				if backoff >= 30*time.Second {
					return fmt.Errorf("error getting block height from peer: %w", err)
				}
				time.Sleep(backoff)
				continue
			}
			backoff = time.Second
			localHeight, err := c.BlockHeight()
			if err != nil {
				return fmt.Errorf("get local block height: %w", err)
			}
			if localHeight >= peerHeight {
				c.log.Printf("synchronized blocks (%d/%d)", localHeight, peerHeight)
				return nil
			}
			c.log.Printf("synchronizing blocks (%d/%d)", localHeight, peerHeight)
		}
	}
}

func (c *Committer) Stop() {
	c.cancel()
}
