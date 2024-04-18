package block_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
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
	goDAproxyGrpc "github.com/rollkit/go-da/proxy/grpc"
	goDATest "github.com/rollkit/go-da/test"
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

	MockDAAddress = "grpc://localhost:7980"
)

func TestMain(m *testing.M) {
	srv := startMockGRPCServ()
	if srv == nil {
		os.Exit(1)
	}

	reg := bitcoin.RegBitcoinProcess{}
	reg.RunBitcoinProcess()
	defer func() {
		srv.GracefulStop()
		reg.Stop()
	}()

	// this will try to run multiple tests at the same time
	exitCode := m.Run()
	os.Exit(exitCode)
}

func startMockGRPCServ() *grpc.Server {
	srv := goDAproxyGrpc.NewServer(goDATest.NewDummyDA(), grpc.Creds(insecure.NewCredentials()))
	addr, _ := url.Parse(MockDAAddress)
	lis, err := net.Listen("tcp", addr.Host)
	if err != nil {
		panic(err)
	}
	go func() {
		_ = srv.Serve(lis)
	}()
	return srv
}

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

	assert.Equal(t, uint64(15), manager.GetBtcProofsHeight())
	for i, block := range blocks {
		assert.Equal(t, uint64(i+1), block.Height)
		assert.Equal(t, fmt.Sprintf("blockproofs-%d", i+1), string(block.BlockProofs))
		assert.Equal(t, fmt.Sprintf("txorderproofs-%d", i+1), string(block.TxOrderProofs))
	}
}

// go test -v -count=1 -run ^TestBtcBlockSubmissionLoop$ github.com/rollkit/rollkit/block
// I observed flakes in this test, where fetching proofs from bitcoin would fail to get all 20 proofs, better run it again
// cached content can mess this up
func TestBtcBlockSubmissionLoop(t *testing.T) {
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

	// block submission loop
	go func() {
		manager.BtcBlockSubmissionLoop(context.Background())
	}()

	// add more blocks
	go func() {
		// save 20 blocks into the block store
		// this should trigger btc block submission loop
		for i := 1; i <= 20; i++ {
			block := types.GetRandomBlock(uint64(i), 10)

			t.Logf("creating block %d with height: %d\n", i, block.Height())

			err := manager.SaveBlock(context.Background(), block, &block.SignedHeader.Commit)
			assert.NoError(t, err)
			// block time of 1 second
			time.Sleep(1 * time.Second)
		}

		wg.Done()
	}()

	go func() {
		manager.BtcRetrieveLoop(context.Background())
	}()

	// try scanning bitcoin layer for 20 submitted roll up proofs
	proofs := []*btctypes.RollUpsBlock{}
	go func() {
		btcBlockChannel := manager.GetBtcBlockInCh()
		for {
			t.Logf("fetching proofs from bitcoin, len = %d \n", len(proofs))
			select {
			case btcBlockEvent := <-btcBlockChannel:
				block := btcBlockEvent.Block
				// basic check before allowed to add into proofs
				if len(block.BlockProofs) != 479 {
					continue
				}

				if len(block.TxOrderProofs) != 32 {
					continue
				}

				proofs = append(proofs, block)
			default:
				manager.SendNonBlockingSignalToRetrieveBtcCh()
			}

			if len(proofs) == 20 {
				break
			}

			time.Sleep(1 * time.Second)
		}

		wg.Done()
	}()

	// wait for all processes to finish
	wg.Wait()

	// ensure that stored local proofs are equal to the ones fetched from bitcoin
	assert.Equal(t, uint64(20), manager.GetBtcProofsHeight())
	for _, proof := range proofs {
		localProofs, err := manager.GetBtcRollUpsBlockFromStore(context.Background(), proof.Height)
		assert.NoError(t, err)
		assert.Equal(t, localProofs.Height, proof.Height)
		assert.Equal(t, localProofs.BlockProofs, proof.BlockProofs)
		assert.Equal(t, localProofs.TxOrderProofs, proof.TxOrderProofs)
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
		BlockTime:             1 * time.Second,
		BtcStartHeight:        uint64(height),
		BtcBlockTime:          regTestBlockTime,
		BtcSignerPriv:         bobPrivateKey,
		BtcSignerInternalPriv: internalPrivateKey,
		BtcNetworkParams:      &chaincfg.RegressionNetParams,
	}

	blockManagerConfig.BtcNetworkParams.DefaultPort = "18443"

	baseKV, err := initBaseKV(config.NodeConfig{}, log.TestingLogger())
	if err != nil {
		return nil, err
	}
	mainKV := newPrefixKV(baseKV, "0")
	store := store.New(mainKV)

	// create a da client
	daClient, err := initDALC(config.NodeConfig{
		DAAddress:   MockDAAddress,
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
