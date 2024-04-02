package block_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	ds "github.com/ipfs/go-datastore"
	ktds "github.com/ipfs/go-datastore/keytransform"
	goDAProxy "github.com/rollkit/go-da/proxy"
	"github.com/rollkit/rollkit/block"
	"github.com/rollkit/rollkit/config"
	"github.com/rollkit/rollkit/da"
	"github.com/rollkit/rollkit/node"
	"github.com/rollkit/rollkit/store"
	"github.com/rollkit/rollkit/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// go test -v -run ^TestFetchBitcoinBlocks$ github.com/rollkit/rollkit/block

func TestFetchBitcoinBlocks(t *testing.T) {
	manager, err := NewMockManager()
	assert.NoError(t, err)
	assert.NotNil(t, manager)

	btcChannel := manager.GetBtcBlockInCh()
	go func() {
		for {
			select {
			case btcBlock := <-btcChannel:
				t.Logf("Received block: %v", btcBlock)
				return
			default:
				t.Logf("No block received")
			}
		}
	}()
	go manager.BtcRetrieveLoop(context.Background())
}

func NewMockManager() (*block.Manager, error) {
	genesisDoc, genesisValidatorKey := types.GetGenesisWithPrivkey()
	signingKey, err := types.PrivKeyToSigningKey(genesisValidatorKey)
	if err != nil {
		return nil, err
	}

	blockManagerConfig := config.BlockManagerConfig{
		BlockTime: 1 * time.Second,
	}

	baseKV, err := initBaseKV(config.NodeConfig{}, log.TestingLogger())
	if err != nil {
		return nil, err
	}
	mainKV := newPrefixKV(baseKV, "0")
	store := store.New(mainKV)

	// create a da client
	daClient, err := initDALC(config.NodeConfig{}, log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	// create a bitcoin client instance
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
	if err != nil {
		return nil, err
	}

	return block.NewManager(
		signingKey,
		blockManagerConfig,
		genesisDoc,
		store,
		nil,
		nil,
		daClient,
		btcClient,
		nil,
		log.NewNopLogger(),
		nil,
		nil,
		nil,
	)
}

func newPrefixKV(kvStore ds.Datastore, prefix string) ds.TxnDatastore {
	return (ktds.Wrap(kvStore, ktds.PrefixTransform{Prefix: ds.NewKey(prefix)}).Children()[0]).(ds.TxnDatastore)
}

// initBaseKV initializes the base key-value store.
func initBaseKV(nodeConfig config.NodeConfig, logger log.Logger) (ds.TxnDatastore, error) {
	if nodeConfig.RootDir == "" && nodeConfig.DBPath == "" { // this is used for testing
		logger.Info("WARNING: working in in-memory mode")
		return store.NewDefaultInMemoryKVStore()
	}
	return store.NewDefaultKVStore(nodeConfig.RootDir, nodeConfig.DBPath, "rollkit")
}

func initDALC(nodeConfig config.NodeConfig, logger log.Logger) (*da.DAClient, error) {
	namespace := make([]byte, len(nodeConfig.DANamespace)/2)
	_, err := hex.Decode(namespace, []byte(nodeConfig.DANamespace))
	if err != nil {
		return nil, fmt.Errorf("error decoding namespace: %w", err)
	}
	daClient := goDAProxy.NewClient()
	err = daClient.Start(nodeConfig.DAAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error while establishing GRPC connection to DA layer: %w", err)
	}
	return &da.DAClient{DA: daClient, Namespace: namespace, GasPrice: nodeConfig.DAGasPrice, GasMultiplier: nodeConfig.DAGasMultiplier, Logger: logger.With("module", "da_client")}, nil
}
