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
)

const (
	bobPrivateKey      = "5JoQtsKQuH8hC9MyvfJAqo6qmKLm8ePYNucs7tPu2YxG12trzBt"
	internalPrivateKey = "5JGgKfRy6vEcWBpLJV5FXUfMGNXzvdWzQHUM1rVLEUJfvZUSwvS"
	regTestBlockTime   = 3 * time.Second
	MockNamespace      = "00000000000000000000000000000000000000000000000000deadbeef"
	MockChainID        = "testnet"
)

func submitStateProofs(t *testing.T, btcClient *bitcoin.BitcoinClient, start, end uint64) {
	state := btctypes.StateProofs{
		Blocks: []*btctypes.RollUpsBlock{},
	}

	for i := start; i < end; i++ {
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

// go test -v -run ^TestSyncBitcoinBlocks$ github.com/rollkit/rollkit/block
func TestSyncBitcoinBlocks(t *testing.T) {
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
	wg.Add(1)

	// function to sync roll up blocks from bitcoin layer
	go func() {
		manager.SyncLoop(context.Background(), nil)
	}()

	// function to retrieve bitcoin blocks
	go func() {
		manager.BtcRetrieveLoop(context.Background())
	}()

	// function to commit roll up blocks
	go func() {
		// send three batches
		var i uint64
		for i = 0; i < 3; i++ {
			submitStateProofs(t, btcClient, i*5+1, (i+1)*5+1)
			time.Sleep(regTestBlockTime)
		}
		wg.Done()
	}()

	// wait for all processes to finish
	wg.Wait()

	height := uint64(1)
	// try fetching all 15 roll ups blocks
	// function to get results from state
	blocks := []*btctypes.RollUpsBlock{}
	for height <= 15 {
		t.Logf("height: %d\n", height)
		block, exists := manager.GetBtcRollUpsBlockFromCache(height)
		if exists {
			blocks = append(blocks, block)
			t.Logf("blocks: %+v\n", blocks)
			height++
		}
	}

	assert.Equal(t, 15, len(blocks))
	for i, block := range blocks {
		assert.Equal(t, uint64(i+1), block.Height)
		assert.Equal(t, fmt.Sprintf("blockproofs-%d", i+1), string(block.BlockProofs))
		assert.Equal(t, fmt.Sprintf("txorderproofs-%d", i+1), string(block.TxOrderProofs))
	}
}

// go test -v -run ^TestBtcBlockSubmissionLoop$ github.com/rollkit/rollkit/block
func TestBtcBlockSubmissionLoop(t *testing.T) {

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
		BtcBlockTime:   regTestBlockTime,
	}

	baseKV, err := initBaseKV(config.NodeConfig{}, log.TestingLogger())
	if err != nil {
		return nil, err
	}
	mainKV := newPrefixKV(baseKV, "0")
	store := store.New(mainKV)

	// create a da client
	daClient, err := initDALC(config.NodeConfig{
		DAAddress:   "grpc://localhost:36650",
		DANamespace: MockNamespace,
		DAAuthToken: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJwdWJsaWMiLCJyZWFkIiwid3JpdGUiLCJhZG1pbiJdfQ.IMt57YStHU4ozdxXu3yH6RJhLCrH1v4rRdPyySOe0bw",
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
	daClient, err := goDAProxy.NewClient(nodeConfig.DAAddress, nodeConfig.DAAuthToken)
	if err != nil {
		return nil, fmt.Errorf("error while creating DA client: %w", err)
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
