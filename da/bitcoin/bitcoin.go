package bitcoin

import (
	"context"

	"github.com/rollkit/rollkit/third_party/log"
)

// BitcoinClient interacts with Bitcoin layer
type BitcoinClient struct {
	Logger log.Logger
}

type ResultSubmitStateProofs struct{}

type StateProofs struct{}

// submit state proofs to bitcoin layer
func (bc *BitcoinClient) SubmitStateProofs(ctx context.Context, stateProofs []byte, gas float64) ResultSubmitStateProofs {
	res := ResultSubmitStateProofs{}
	return res
}

// retrieve state proofs from bitcoin layer
func (bc *BitcoinClient) RetrieveStateProofs(ctx context.Context) StateProofs {
	res := StateProofs{}
	return res
}
