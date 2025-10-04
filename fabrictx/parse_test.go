package fabrictx_test

import (
	"os"
	"testing"

	"github.com/arner/hacky-fabric/fabrictx"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

func TestParseEndorsed(t *testing.T) {
	b, err := os.ReadFile("./fixtures/endorsed.block")
	if err != nil {
		t.Fatal(err)
	}

	block := &common.Block{}
	if err = proto.Unmarshal(b, block); err != nil {
		t.Fatal(err)
	}
	env := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[0], env); err != nil {
		t.Fatal(err)
	}
	e, err := fabrictx.EndorserTxToStruct(env)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(e.String())
}
