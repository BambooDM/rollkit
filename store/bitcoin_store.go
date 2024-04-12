package store

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	ds "github.com/ipfs/go-datastore"
	"github.com/rollkit/rollkit/types"
	btctypes "github.com/rollkit/rollkit/types/pb/bitcoin"
)

// BtcStore is minimal interface extending Store for storing and retrieving Bitcoin-specific data.
type BtcStore interface {
	BtcRollupsProofsHeight() uint64

	SetBtcRollupsProofsHeight(ctx context.Context, height uint64)

	SetBtcRollupsProofs(ctx context.Context, proofs *btctypes.RollUpsBlock) error

	GetBtcRollupsProofs(ctx context.Context, height uint64) (*btctypes.RollUpsBlock, error)
}

func (s *DefaultStore) BtcRollupsProofsHeight() uint64 {
	return s.btcRollupsProofsHeight.Load()
}

func (s *DefaultStore) SetBtcRollupsProofsHeight(ctx context.Context, height uint64) {
	for {
		storeHeight := s.btcRollupsProofsHeight.Load()
		if height <= storeHeight {
			break
		}
		if s.btcRollupsProofsHeight.CompareAndSwap(storeHeight, height) {
			break
		}
	}
}

func (s *DefaultStore) SetBtcRollupsProofs(ctx context.Context, proofs *btctypes.RollUpsBlock) error {
	proofsBlob, err := proofs.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal Block to binary: %w", err)
	}

	bb, err := s.db.NewTransaction(ctx, false)
	if err != nil {
		return fmt.Errorf("failed to create a new batch for transaction: %w", err)
	}
	defer bb.Discard(ctx)

	err = bb.Put(ctx, ds.NewKey(getBtcProofsKey(proofs.BlockProofs)), proofsBlob)
	if err != nil {
		return fmt.Errorf("failed to create a new key for proofs Blob: %w", err)
	}
	err = bb.Put(ctx, ds.NewKey(getBtcProofsIndexKey(proofs.Height)), proofs.BlockProofs[:])
	if err != nil {
		return fmt.Errorf("failed to create a new key using height of the block: %w", err)
	}

	if err = bb.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *DefaultStore) GetBtcRollupsProofs(ctx context.Context, height uint64) (*btctypes.RollUpsBlock, error) {
	h, err := s.loadBtcProofsHashFromIndex(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash from index: %w", err)
	}
	return s.GetBtcRollupsProofsByHash(ctx, h)
}

// GetBlockByHash returns block with given block header hash, or error if it's not found in Store.
func (s *DefaultStore) GetBtcRollupsProofsByHash(ctx context.Context, hash []byte) (*btctypes.RollUpsBlock, error) {
	proofsData, err := s.db.Get(ctx, ds.NewKey(getBtcProofsKey(hash)))
	if err != nil {
		return nil, fmt.Errorf("failed to load block data: %w", err)
	}
	proofs := new(btctypes.RollUpsBlock)
	err = proofs.Unmarshal(proofsData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal block data: %w", err)
	}

	return proofs, nil
}

// loadHashFromIndex returns the hash of a block given its height
func (s *DefaultStore) loadBtcProofsHashFromIndex(ctx context.Context, height uint64) ([]byte, error) {
	blob, err := s.db.Get(ctx, ds.NewKey(getBtcProofsIndexKey(height)))

	if err != nil {
		return nil, fmt.Errorf("failed to load btc proofs hash for height %v: %w", height, err)
	}
	return blob, nil
}

func getBtcProofsKey(hash types.Hash) string {
	return GenerateKey([]string{btcProofsPrefix, hex.EncodeToString(hash[:])})
}

func getBtcProofsIndexKey(height uint64) string {
	return GenerateKey([]string{btcProofsIndexPrefix, strconv.FormatUint(height, 10)})
}
