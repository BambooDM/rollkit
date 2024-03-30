package bitcoin_test

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/rollkit/rollkit/config"
	"github.com/rollkit/rollkit/da/bitcoin"
	"github.com/rollkit/rollkit/node"
	btctypes "github.com/rollkit/rollkit/types/pb/bitcoin"
	"github.com/stretchr/testify/assert"
)

const (
	bobPrivateKey      = "5JoQtsKQuH8hC9MyvfJAqo6qmKLm8ePYNucs7tPu2YxG12trzBt"
	internalPrivateKey = "5JGgKfRy6vEcWBpLJV5FXUfMGNXzvdWzQHUM1rVLEUJfvZUSwvS"
	regTestBlockTime   = 3 * time.Second
)

// go test -v -run ^TestRetrieveBlocks$ github.com/rollkit/rollkit/da/bitcoin
func TestRetrieveBlocks(t *testing.T) {
	t.Run("TestRetrieveBlocks", func(t *testing.T) {
		nodeConfig := config.NodeConfig{
			// regtest network
			// host: "localhost:18443"
			BitcoinManagerConfig: config.BitcoinManagerConfig{
				BtcHost: "0.0.0.0:18443",
				BtcUser: "regtest",
				BtcPass: "regtest",
				// enable http post mode which is bitcoin node default
				BtcHTTPPostMode: true,
				BtcDisableTLS:   true,
			},
		}
		btcClient, err := node.InitBitcoinClient(nodeConfig, log.NewNopLogger())
		assert.NoError(t, err, "Have you started a bitcoin regtest node?")
		assert.NotNil(t, btcClient)

		// submit state proofs
		stateProofs := btctypes.StateProofs{
			Blocks: []*btctypes.RollUpsBlock{
				{
					BlockProofs:   []byte("blockproofs"),
					TxOrderProofs: []byte("txorderproofs"),
				},
			},
		}

		latestBlockHeight, err := btcClient.BtcClient.GetBlockCount()
		assert.NoError(t, err)
		_ = SendStateProofs(btcClient, stateProofs, t)

		// retrieve blocks starting from latest block height
		var res bitcoin.ResultRetrieveBlocks
		pointer := latestBlockHeight
		for {
			res = btcClient.RetrieveBlocks(context.Background(), pointer)
			if res.Code == bitcoin.StatusSuccess {
				break
			}
			pointer++
			time.Sleep(regTestBlockTime)
		}

		assert.Equal(t, bitcoin.StatusSuccess, res.Code)
		assert.Equal(t, len(res.StateProofs.Blocks), 1)
	})
}

// go test -v -run ^TestReadTransaction$ github.com/rollkit/rollkit/da/bitcoin
func TestReadTransaction(t *testing.T) {
	t.Run("TestSubmitStateProofs", func(t *testing.T) {
		nodeConfig := config.NodeConfig{
			// regtest network
			// host: "localhost:18443"
			BitcoinManagerConfig: config.BitcoinManagerConfig{
				BtcHost: "0.0.0.0:18443",
				BtcUser: "regtest",
				BtcPass: "regtest",
				// enable http post mode which is bitcoin node default
				BtcHTTPPostMode: true,
				BtcDisableTLS:   true,
			},
		}

		btcClient, err := node.InitBitcoinClient(nodeConfig, log.NewNopLogger())
		assert.NoError(t, err, "Have you started a bitcoin regtest node?")
		assert.NotNil(t, btcClient)

		// submit state proofs
		stateProofs := btctypes.StateProofs{
			Blocks: []*btctypes.RollUpsBlock{
				{
					BlockProofs:   []byte("blockproofs"),
					TxOrderProofs: []byte("txorderproofs"),
				},
			},
		}

		submitHash := SendStateProofs(btcClient, stateProofs, t)

		// read submitted transaction
		resp, err := btcClient.ReadTransaction(submitHash)
		assert.NoError(t, err)
		stateProofsResp := &btctypes.StateProofs{}
		err = stateProofsResp.Unmarshal(resp)
		assert.NoError(t, err)

		assert.Equal(t, stateProofs.Blocks[0].BlockProofs, stateProofsResp.Blocks[0].BlockProofs)
	})
}

// submit state proofs
func SendStateProofs(btcClient *bitcoin.BitcoinClient, stateProofs btctypes.StateProofs, t *testing.T) *chainhash.Hash {
	chaincfg := &chaincfg.RegressionNetParams
	chaincfg.DefaultPort = "18443"

	// submit state proofs bytes
	res := btcClient.SubmitStateProofs(
		context.Background(),
		stateProofs,
		bobPrivateKey,
		internalPrivateKey,
		chaincfg,
	)
	assert.Equal(t, bitcoin.StatusSuccess, res.Code)

	return res.SubmitHash
}
