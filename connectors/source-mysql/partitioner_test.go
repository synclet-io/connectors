package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEstimatePartitions(t *testing.T) {
	const mb = 1024 * 1024
	const partitionSizeBytes = 50 * mb

	tests := []struct {
		name       string
		dataLength int64
		maxWorkers int
		want       int
	}{
		{"single worker forces 1 partition regardless of size", 10 * 1024 * partitionSizeBytes, 1, 1},
		{"zero workers normalized to 1", 100 * partitionSizeBytes, 0, 1},
		{"negative workers normalized to 1", 100 * partitionSizeBytes, -3, 1},
		{"tiny table fits in 1 partition", 1024, 8, 1},
		{"exactly one partition worth of data", partitionSizeBytes, 4, 1},
		{"two partitions worth of data fits 2", 2 * partitionSizeBytes, 4, 2},
		{"capped by maxWorkers", 100 * partitionSizeBytes, 4, 4},
		{"max equals natural partition count", 4 * partitionSizeBytes, 4, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, estimatePartitions(tc.dataLength, tc.maxWorkers))
		})
	}
}
