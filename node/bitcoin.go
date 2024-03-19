package node

import (
	"github.com/rollkit/rollkit/da/bitcoin"
	"github.com/rollkit/rollkit/third_party/log"
)

func initBitcoinClient(logger log.Logger) (*bitcoin.BitcoinClient, error) {
	// create a new bitcoin client
	bitcoinClient := &bitcoin.BitcoinClient{
		Logger: logger,
	}

	return bitcoinClient, nil
}
