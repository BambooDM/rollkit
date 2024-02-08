package mock

import (
	"context"

	"github.com/rollkit/go-da"
	"github.com/stretchr/testify/mock"
)

// MockDA is a mock for the DA interface
type MockDA struct {
	mock.Mock
}

func (m *MockDA) MaxBlobSize(ctx context.Context) (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockDA) Get(ctx context.Context, ids []da.ID) ([]da.Blob, error) {
	args := m.Called(ids)
	return args.Get(0).([]da.Blob), args.Error(1)
}

func (m *MockDA) GetIDs(ctx context.Context, height uint64) ([]da.ID, error) {
	args := m.Called(height)
	return args.Get(0).([]da.ID), args.Error(1)
}

func (m *MockDA) Commit(ctx context.Context, blobs []da.Blob) ([]da.Commitment, error) {
	args := m.Called(blobs)
	return args.Get(0).([]da.Commitment), args.Error(1)
}

func (m *MockDA) Submit(ctx context.Context, blobs []da.Blob, gasPrice float64) ([]da.ID, []da.Proof, error) {
	args := m.Called(blobs, gasPrice)
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
		return args.Get(0).([]da.ID), args.Get(1).([]da.Proof), args.Error(2)
	}
}

func (m *MockDA) Validate(ctx context.Context, ids []da.ID, proofs []da.Proof) ([]bool, error) {
	args := m.Called(ids, proofs)
	return args.Get(0).([]bool), args.Error(1)
}
