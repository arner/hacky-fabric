# Hacky Fabric

A set of code and tools to play around with Hyperledger Fabric on a fairly low level (grpc, protos, with few abstractions). None of it is finished or production ready, but the components can be helpful to use and adapt for prototyping or learning.

Features / components:

- Convert protobuf transactions to struct and json.
- Create valid endorsed transactions with arbitrary read/write sets offline (without talking to a peer).
- Basic clients to talk to an orderer (to submit transactions) or peer (for query and subscribe for new blocks).
- A committer service that connects to a peer and stores all the committed writes in a local sqlite or postgres database.
- A "stub" that can read from that same database and form read/write sets based on GetState, PutState and DelState calls.

## Get started

To try out the components in integration tests or a custom application, make sure you have cloned [fabric samples](git@github.com:hyperledger/fabric-samples.git) and exported its location.

```shell
git clone git@github.com:hyperledger/fabric-samples.git
export FABRIC_SAMPLES="$(pwd)/fabric-samples"
```

With the following commands the network, install a chaincode, copy the keys and execute the integration tests.

```shell
"$FABRIC_SAMPLES/test-network/network.sh" up createChannel -i 3.1.1
"$FABRIC_SAMPLES/test-network/network.sh" deployCCAAS -ccn basicts -ccp "$FABRIC_SAMPLES/asset-transfer-basic/chaincode-typescript"

rm -rf integration/keys && mkdir -p integration/keys
cp -r "$FABRIC_SAMPLES/test-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp" "integration/keys/user"
cp -r "$FABRIC_SAMPLES/test-network/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/msp" "integration/keys/endorser"
cp -r "$FABRIC_SAMPLES/test-network/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/msp" "integration/keys/endorser2"

cp -r "$FABRIC_SAMPLES/test-network/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt" "integration/keys/peer-tls-ca.crt"
cp -r "$FABRIC_SAMPLES/test-network/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/tls/ca.crt" "integration/keys/orderer-tls-ca.crt"

go test ./integration
```

The chaincode is there not necessarily to execute, but to create a namespace on the ledger. Unfortuanately it is
not possible to create the namespace with custom envelopes directly. It requires a write in the lscc namespace which
is only possible from within the peer process itself, following the chaincode lifecycle process.

To tear down:

```shell
"$FABRIC_SAMPLES/test-network/network.sh" down
rm -rf integration/keys
```

## Examples

#### Create, sign and submit a transaction (errors omitted)

```go
submitter, _ := fabrictx.SignerFromMSP("keys/user", "Org1MSP")
endorser1, _ := fabrictx.SignerFromMSP("keys/endorser", "Org1MSP")
endorser2, _ := fabrictx.SignerFromMSP("keys/endorser2", "Org2MSP")

pem, _ := os.ReadFile(path.Join(dir, "orderer-tls-ca.crt"))
orderer, _ := comm.NewOrderer("orderer.example.com:7050", pem)
defer orderer.Close()

rw := &kvrwset.KVRWSet{
  Writes: []*kvrwset.KVWrite{
    {Key: "my_test_key", Value: []byte(`{"MyVal":"hello world"}`)},
  },
}
tx, id, _ := fabrictx.NewEndorserTransaction("mychannel", "basicts", submitter, []fabrictx.Signer{endorser1, endorser2}, rw)
orderer.Broadcast(tx)
```

#### Format of a parsed transaction

```json
{
  "payload": {
    "header": {
      "channel_header": {
        "type": 3,
        "timestamp": {
          "seconds": 1759490165
        },
        "channel_id": "mychannel",
        "tx_id": "43980a2261b92f4d111d2509d111ffb23b1b3d00d3263caa0b18a92fc1ca7bfc",
        "extension": "Eg4SB2Jhc2ljdHMaAzEuMA=="
      },
      "signature_header": {
        "creator": "CgdPcmcxTVNQEqoGLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNLVENDQWRDZ0F3SUJBZ0lRRHVwcGgvR1NEM29FMEhFL290K0ptREFLQmdncWhrak9QUVFEQWpCek1Rc3cKQ1FZRFZRUUdFd0pWVXpFVE1CRUdBMVVFQ0JNS1EyRnNhV1p2Y201cFlURVdNQlFHQTFVRUJ4TU5VMkZ1SUVaeQpZVzVqYVhOamJ6RVpNQmNHQTFVRUNoTVFiM0puTVM1bGVHRnRjR3hsTG1OdmJURWNNQm9HQTFVRUF4TVRZMkV1CmIzSm5NUzVsZUdGdGNHeGxMbU52YlRBZUZ3MHlOVEV3TURNd09UTTNNREJhRncwek5URXdNREV3T1RNM01EQmEKTUd3eEN6QUpCZ05WQkFZVEFsVlRNUk13RVFZRFZRUUlFd3BEWVd4cFptOXlibWxoTVJZd0ZBWURWUVFIRXcxVApZVzRnUm5KaGJtTnBjMk52TVE4d0RRWURWUVFMRXdaamJHbGxiblF4SHpBZEJnTlZCQU1NRmxWelpYSXhRRzl5Clp6RXVaWGhoYlhCc1pTNWpiMjB3V1RBVEJnY3Foa2pPUFFJQkJnZ3Foa2pPUFFNQkJ3TkNBQVI4bVBlOWdyLzQKYlFUSnIzRFNJS0JQS2ZMeU5DNnBHV3lYbXJ6L3I3YUwwRm15eVFrbkdSV2JSN2orRUZGNTVmM1ZUT1krZ2gyRQpRcTRIOTBDbWRoc29vMDB3U3pBT0JnTlZIUThCQWY4RUJBTUNCNEF3REFZRFZSMFRBUUgvQkFJd0FEQXJCZ05WCkhTTUVKREFpZ0NBOEcrOXhGTTVzWjZPR0Z4SDUyQ1B3b3UxbkV6RC8rbzE5ZzFBM1k4QWtzekFLQmdncWhrak8KUFFRREFnTkhBREJFQWlCWE02bVYxSWRONDZNSWgwajY4SEtLdXpoT2NMeWJNczJ2TGlVTi9IT0Nyd0lnUDQz
MQp4RlAydUJzaDhleU56S1l4M3FBNGpndHhaM0VRSnFJRDVhSzVpejQ9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K",
        "nonce": "Vk8Ia15pZoQrKuIxjC94jFVLu8tBTtTB"
      }
    },
    "data": {
      "actions": [
        {
          "chaincode_proposal_payload": {
            "input": {
              "chaincode_spec": {
                "type": 2,
                "chaincode_id": {
                  "name": "basicts",
                  "version": "1.0"
                },
                "input": {
                  "args": [
                    "ZnVuY3Rpb25fbmFtZQ=="
                  ]
                }
              }
            }
          },
          "endorsements": [
            {
              "Endorser": {
                "mspid": "Org1MSP",
                "id_bytes": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNKekNDQWM2Z0F3SUJBZ0lRV0pnc2tyd21LQldzOG1JUjdaT0FCekFLQmdncWhrak9QUVFEQWpCek1Rc3cKQ1FZRFZRUUdFd0pWVXpFVE1CRUdBMVVFQ0JNS1EyRnNhV1p2Y201cFlURVdNQlFHQTFVRUJ4TU5VMkZ1SUVaeQpZVzVqYVhOamJ6RVpNQmNHQTFVRUNoTVFiM0puTVM1bGVHRnRjR3hsTG1OdmJURWNNQm9HQTFVRUF4TVRZMkV1CmIzSm5NUzVsZUdGdGNHeGxMbU52YlRBZUZ3MHlOVEV3TURNd09UTTNNREJhRncwek5URXdNREV3T1RNM01EQmEKTUdveEN6QUpCZ05WQkFZVEFsVlRNUk13RVFZRFZRUUlFd3BEWVd4cFptOXlibWxoTVJZd0ZBWURWUVFIRXcxVApZVzRnUm5KaGJtTnBjMk52TVEwd0N3WURWUVFMRXdSd1pXVnlNUjh3SFFZRFZRUURFeFp3WldWeU1DNXZjbWN4CkxtVjRZVzF3YkdVdVkyOXRNRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVCanlHczIxRDFyYm8KUWZIYTFGZjU1R0V1eWp5MFlWNzJyMyt5SzdVekE1UWN0bkJ4blAzbWgvV2lhSTI1RkhFbFVQTXVNMWkvRDdyOQpYMFYxVEl6eDZhTk5NRXN3RGdZRFZSMFBBUUgvQkFRREFnZUFNQXdHQTFVZEV3RUIvd1FDTUFBd0t3WURWUjBqCkJDUXdJb0FnUEJ2dmNSVE9iR2VqaGhjUitkZ2o4S0x0WnhNdy8vcU5mWU5RTjJQQUpMTXdDZ1lJS29aSXpqMEUKQXdJRFJ3QXdSQUlnZitIRDlqUE9HOWtIMXYzVDJGblEraGZ6c0g5WkJyNHNKdXg1RC8rVmEvVUNJRjZSZllEVwpBQ3N
4LzRscWowOVVjaldiZFVsL3NCNnpHem0yNTZOM3daLzUKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo="
              },
              "Signature": "MEQCICHK4/Fo8EMfXGJ2rjVLDfRULow18OOA6ucUbuCZYxqOAiB5+FZ+roartDystfzsn1lS4V8t32VqvDQHc/DLh/7RQQ=="
            },
            {
              "Endorser": {
                "mspid": "Org2MSP",
                "id_bytes": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNLRENDQWM2Z0F3SUJBZ0lRR1lML3ZReHVZbkt5a1l2QmNHSmxrREFLQmdncWhrak9QUVFEQWpCek1Rc3cKQ1FZRFZRUUdFd0pWVXpFVE1CRUdBMVVFQ0JNS1EyRnNhV1p2Y201cFlURVdNQlFHQTFVRUJ4TU5VMkZ1SUVaeQpZVzVqYVhOamJ6RVpNQmNHQTFVRUNoTVFiM0puTWk1bGVHRnRjR3hsTG1OdmJURWNNQm9HQTFVRUF4TVRZMkV1CmIzSm5NaTVsZUdGdGNHeGxMbU52YlRBZUZ3MHlOVEV3TURNd09UTTNNREJhRncwek5URXdNREV3T1RNM01EQmEKTUdveEN6QUpCZ05WQkFZVEFsVlRNUk13RVFZRFZRUUlFd3BEWVd4cFptOXlibWxoTVJZd0ZBWURWUVFIRXcxVApZVzRnUm5KaGJtTnBjMk52TVEwd0N3WURWUVFMRXdSd1pXVnlNUjh3SFFZRFZRUURFeFp3WldWeU1DNXZjbWN5CkxtVjRZVzF3YkdVdVkyOXRNRmt3RXdZSEtvWkl6ajBDQVFZSUtvWkl6ajBEQVFjRFFnQUVuK2hLRWhDeUVZZFkKeG15N0FVaUpnMHpZSktMK2k0R09mZ2t4TklxcG5tc0sxR05hZ3htMGZKQ0EzVXNPaWI5N3pJYzc0VFdVZ3N4ZgpUQnJxL2hUTmJxTk5NRXN3RGdZRFZSMFBBUUgvQkFRREFnZUFNQXdHQTFVZEV3RUIvd1FDTUFBd0t3WURWUjBqCkJDUXdJb0FnSkt0ajFBT2h1emh5ZnNLRFJseFZITUlxbGN4aG01N2F0cWMvSEhIdENPUXdDZ1lJS29aSXpqMEUKQXdJRFNBQXdSUUloQUswR2pTNHpTR1ZDTGtPYnVvOU95NzZJSyt1ajBmRkY5NGMyQkgzT0hxMXFBaUJ2dFloQgplTjd
uQ29yS2dFSlFrNU1RT1Jwd0lqT05JRlVaNTFGdER6MGpFZz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
              }
            }
          ],
          "proposal_response_payload": {
            "proposal_hash": "CdT2FCX/MOAMVMUuwSkSx+8WdhdIB1jSX5K0YvPvJdM=",
            "extension": {
              "chaincode_id": {
                "name": "basicts",
                "version": "1.0"
              },
              "response": {
                "status": 200,
                "message": "OK"
              },
              "events": {},
              "results": [
                {
                  "namespace": "basicts",
                  "reads": null,
                  "writes": [
                    {
                      "key": "my_test_key",
                      "value": "eyJNeVZhbCI6ImhlbGxvIHdvcmxkIn0="
                    }
                  ]
                }
              ]
            }
          }
        }
      ]
    }
  },
  "signature": "MEUCIQCoNGs6Pvxc4BhERQHqn/tZZsxSc+YZllpUt9E6N23xsgIgSP1nZcxwyO4uAZsmmxuKQluevOc4mM2jHqyOC205nPw="
}
```

#### Keep a local copy of the world state and history

```go
// initialize the world state store
db, _ := sql.Open("sqlite", "./worldstate.sqlite")
store := committer.NewStorage("mychannel", db, "sqlite")
store.Init()

// start the committer
committer, err := committer.NewCommitter(ctx, store, "mychannel", peer, submitter, log.New(os.Stdout, "committer:", log.LstdFlags))
go committer.Run() // process blocks in the background
committer.WaitUntilSynced(ctx) // block the thread until the committer is fully synced with the peer

height, _ := committer.BlockHeight()
logger.Println("blockheight is %d", height)

// ...

committer.Stop()
```
