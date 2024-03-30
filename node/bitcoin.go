package node

import (
	"fmt"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/rollkit/rollkit/config"
	"github.com/rollkit/rollkit/da/bitcoin"
	"github.com/rollkit/rollkit/third_party/log"
)

func InitBitcoinClient(nodeConfig config.NodeConfig, logger log.Logger) (*bitcoin.BitcoinClient, error) {
	connCfg := &rpcclient.ConnConfig{
		Host:         nodeConfig.BtcHost,
		User:         nodeConfig.BtcUser,
		Pass:         nodeConfig.BtcPass,
		HTTPPostMode: nodeConfig.BtcHTTPPostMode,
		DisableTLS:   nodeConfig.BtcDisableTLS,
	}

	// todo: determine what to do with bitcoin events for notification handlers
	btcClient, err := rpcclient.New(connCfg, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating btcd RPC client: %v", err)
	}

	// create a new bitcoin client
	bitcoinClient := &bitcoin.BitcoinClient{
		Logger:    logger,
		BtcClient: btcClient,
	}

	return bitcoinClient, nil
}
