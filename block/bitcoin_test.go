package block_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	abci "github.com/cometbft/cometbft/abci/types"
	cmconfig "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/proxy"
	ds "github.com/ipfs/go-datastore"
	ktds "github.com/ipfs/go-datastore/keytransform"
	goDAProxy "github.com/rollkit/go-da/proxy"
	"github.com/rollkit/rollkit/block"
	"github.com/rollkit/rollkit/config"
	"github.com/rollkit/rollkit/da"
	"github.com/rollkit/rollkit/da/bitcoin"
	"github.com/rollkit/rollkit/node"
	"github.com/rollkit/rollkit/store"
	"github.com/rollkit/rollkit/test/mocks"
	"github.com/rollkit/rollkit/types"
	btctypes "github.com/rollkit/rollkit/types/pb/bitcoin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	bobPrivateKey      = "5JoQtsKQuH8hC9MyvfJAqo6qmKLm8ePYNucs7tPu2YxG12trzBt"
	internalPrivateKey = "5JGgKfRy6vEcWBpLJV5FXUfMGNXzvdWzQHUM1rVLEUJfvZUSwvS"
	regTestBlockTime   = 3 * time.Second
	MockNamespace      = "00000000000000000000000000000000000000000000000000deadbeef"
	MockChainID        = "testnet"
)

func submitStateProofs(t *testing.T, btcClient *bitcoin.BitcoinClient) {
	state := btctypes.StateProofs{
		Blocks: []*btctypes.RollUpsBlock{},
	}

	for i := 0; i < 5; i++ {
		state.Blocks = append(state.Blocks, &btctypes.RollUpsBlock{
			BlockProofs:   []byte(fmt.Sprintf("blockproofs-%d", i)),
			TxOrderProofs: []byte(fmt.Sprintf("txorderproofs-%d", i)),
			Height:        uint64(i),
		})
	}

	chaincfg := &chaincfg.RegressionNetParams
	chaincfg.DefaultPort = "18443"

	res := btcClient.SubmitStateProofs(context.Background(), state, bobPrivateKey, internalPrivateKey, chaincfg)
	assert.Equal(t, bitcoin.StatusSuccess, res.Code)
	t.Logf("SubmitStateProofs: %+v\n", res)
}

// go test -v -run ^TestFetchBitcoinBlocks$ github.com/rollkit/rollkit/block
func TestFetchBitcoinBlocks(t *testing.T) {
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
	assert.NoError(t, err)

	manager, err := NewMockManager(btcClient)
	assert.NoError(t, err)
	assert.NotNil(t, manager)

	var wg sync.WaitGroup
	wg.Add(2)

	btcChannel := manager.GetBtcBlockInCh()
	blocks := []block.NewBtcBlockEvent{}
	// function to get roll up blocks from bitcoin layer
	go func() {
		for {
			select {
			case btcBlock := <-btcChannel:
				t.Logf("Received block: %+v", btcBlock)
				blocks = append(blocks, btcBlock)
			default:
				t.Logf("No block received")
			}

			if len(blocks) == 5 {
				break
			}

			time.Sleep(3 * time.Second)
		}

		wg.Done()
	}()

	// function to retrieve bitcoin blocks
	go func() {
		manager.BtcRetrieveLoop(context.Background())
	}()

	// function to commit roll up blocks
	go func() {
		submitStateProofs(t, btcClient)
		wg.Done()
	}()

	// wait for all processes to finish
	wg.Wait()

	assert.Equal(t, 5, len(blocks))
	for i, block := range blocks {
		assert.Equal(t, uint64(i), block.Block.Height)
		assert.Equal(t, fmt.Sprintf("blockproofs-%d", i), string(block.Block.BlockProofs))
		assert.Equal(t, fmt.Sprintf("txorderproofs-%d", i), string(block.Block.TxOrderProofs))
	}
}

func NewMockManager(btc *bitcoin.BitcoinClient) (*block.Manager, error) {
	genesisDoc, genesisValidatorKey := types.GetGenesisWithPrivkey()
	signingKey, err := types.PrivKeyToSigningKey(genesisValidatorKey)
	if err != nil {
		return nil, err
	}

	height, err := btc.BtcClient.GetBlockCount()
	if err != nil {
		return nil, err
	}

	blockManagerConfig := config.BlockManagerConfig{
		BlockTime:      1 * time.Second,
		BtcStartHeight: uint64(height),
	}

	baseKV, err := initBaseKV(config.NodeConfig{}, log.TestingLogger())
	if err != nil {
		return nil, err
	}
	mainKV := newPrefixKV(baseKV, "0")
	store := store.New(mainKV)

	// create a da client
	daClient, err := initDALC(config.NodeConfig{
		DAAddress:   "localhost:36650",
		DANamespace: MockNamespace,
	}, log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	// create proxy app
	mockApp := &mocks.Application{}
	mockApp.On("InitChain", mock.Anything, mock.Anything).Return(&abci.ResponseInitChain{}, nil)
	_, _, _, _, abciMetrics := node.DefaultMetricsProvider(cmconfig.DefaultInstrumentationConfig())(MockChainID)
	proxyApp, err := initProxyApp(proxy.NewLocalClientCreator(mockApp), log.NewNopLogger(), abciMetrics)
	if err != nil {
		return nil, err
	}

	return block.NewManager(
		signingKey,
		blockManagerConfig,
		genesisDoc,
		store,
		nil,
		proxyApp.Consensus(),
		daClient,
		btc,
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

// need to run a DA layer
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

func initProxyApp(clientCreator proxy.ClientCreator, logger log.Logger, metrics *proxy.Metrics) (proxy.AppConns, error) {
	proxyApp := proxy.NewAppConns(clientCreator, metrics)
	proxyApp.SetLogger(logger.With("module", "proxy"))
	if err := proxyApp.Start(); err != nil {
		return nil, fmt.Errorf("error while starting proxy app connections: %v", err)
	}
	return proxyApp, nil
}
