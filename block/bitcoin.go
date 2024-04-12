package block

import (
	"crypto/sha256"
	"sync"

	"github.com/rollkit/rollkit/types"
	btctypes "github.com/rollkit/rollkit/types/pb/bitcoin"
)

// BlockCache maintains blocks that are seen and hard confirmed
type BtcBlockCache struct {
	blocks      *sync.Map
	hashes      *sync.Map
	btcIncluded *sync.Map
}

// NewBtcBlockCache returns a new BlockCache struct
func NewBtcBlockCache() *BtcBlockCache {
	return &BtcBlockCache{
		blocks:      new(sync.Map),
		hashes:      new(sync.Map),
		btcIncluded: new(sync.Map),
	}
}

func (bc *BtcBlockCache) getBlock(height uint64) (*btctypes.RollUpsBlock, bool) {
	block, ok := bc.blocks.Load(height)
	if !ok {
		return nil, false
	}
	return block.(*btctypes.RollUpsBlock), true
}

func (bc *BtcBlockCache) setBlock(height uint64, block *btctypes.RollUpsBlock) {
	if block != nil {
		bc.blocks.Store(height, block)
	}
}

func (bc *BtcBlockCache) deleteBlock(height uint64) {
	bc.blocks.Delete(height)
}

func (bc *BtcBlockCache) isSeen(hash string) bool {
	seen, ok := bc.hashes.Load(hash)
	if !ok {
		return false
	}
	return seen.(bool)
}

func (bc *BtcBlockCache) setSeen(hash string) {
	bc.hashes.Store(hash, true)
}

func (bc *BtcBlockCache) isBtcIncluded(hash string) bool {
	btcIncluded, ok := bc.btcIncluded.Load(hash)
	if !ok {
		return false
	}
	return btcIncluded.(bool)
}

func (bc *BtcBlockCache) setBtcIncluded(hash string) {
	bc.btcIncluded.Store(hash, true)
}

func ConvertBlockToProofs(block *types.Block) (*btctypes.RollUpsBlock, error) {
	// construct proofs for btc
	signedHeader, err := block.SignedHeader.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// sha256 of all txs to ensure ordering
	var txOrderProofs [32]byte
	var combinedTxs []byte
	for _, txBytes := range block.Data.Txs.ToSliceOfBytes() {
		combinedTxs = append(combinedTxs, txBytes...)
	}
	txOrderProofs = sha256.Sum256(combinedTxs)

	proof := &btctypes.RollUpsBlock{
		BlockProofs:   signedHeader,
		TxOrderProofs: txOrderProofs[:],
		Height:        block.Height(),
	}

	return proof, nil
}
