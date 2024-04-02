package block

import (
	"context"
	"testing"
)

func TestFetchBitcoinBlocks(t *testing.T) {
	manager := NewMockManager()

	go manager.BtcRetrieveLoop(context.Background())
}

func NewMockManager() *Manager {
	return &Manager{}
}
