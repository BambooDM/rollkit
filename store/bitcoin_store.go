package store

import "context"

// BtcStore is minimal interface extending Store for storing and retrieving Bitcoin-specific data.
type BtcStore interface {
	// Get the lastest roll ups block height retrieved from Bitcoin
	BtcRollupsHeight() uint64

	// Set the lastest roll ups block height retrieved from Bitcoin
	SetBtcRollupsHeight(ctx context.Context, height uint64)
}

func (s *DefaultStore) BtcRollupsHeight() uint64 {
	return s.btcRollupsHeight.Load()
}

func (s *DefaultStore) SetBtcRollupsHeight(ctx context.Context, height uint64) {
	for {
		storeHeight := s.btcRollupsHeight.Load()
		if height <= storeHeight {
			break
		}
		if s.btcRollupsHeight.CompareAndSwap(storeHeight, height) {
			break
		}
	}
}
