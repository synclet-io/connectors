package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSheetOps tracks calls to sheetOps methods for verifying overwrite/append behavior.
type mockSheetOps struct {
	ensuredSheets []string
	clearedSheets []string
	appendedCalls []appendCall
}

type appendCall struct {
	sheetTitle string
	rows       [][]interface{}
}

func (m *mockSheetOps) ensureSheet(sheetTitle string) error {
	m.ensuredSheets = append(m.ensuredSheets, sheetTitle)
	return nil
}

func (m *mockSheetOps) clearSheet(sheetTitle string) error {
	m.clearedSheets = append(m.clearedSheets, sheetTitle)
	return nil
}

func (m *mockSheetOps) appendRows(sheetTitle string, rows [][]interface{}) error {
	m.appendedCalls = append(m.appendedCalls, appendCall{sheetTitle: sheetTitle, rows: rows})
	return nil
}

func TestSheetWriter_HeadersFromFirstRecord(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 100)

	err := w.AddRecord("stream1", map[string]interface{}{
		"name": "Alice",
		"age":  "30",
		"city": "NYC",
	}, "overwrite")
	require.NoError(t, err)

	buf := w.buffers["stream1"]
	require.NotNil(t, buf)
	// Headers should be sorted alphabetically
	assert.Equal(t, []string{"age", "city", "name"}, buf.headers)
}

func TestSheetWriter_ColumnOrderConsistency(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 100)

	// First record establishes order
	err := w.AddRecord("stream1", map[string]interface{}{
		"z_col": "1",
		"a_col": "2",
		"m_col": "3",
	}, "overwrite")
	require.NoError(t, err)

	// Second record uses same order
	err = w.AddRecord("stream1", map[string]interface{}{
		"a_col": "x",
		"z_col": "y",
		"m_col": "z",
	}, "overwrite")
	require.NoError(t, err)

	buf := w.buffers["stream1"]
	assert.Equal(t, []string{"a_col", "m_col", "z_col"}, buf.headers)
	assert.Equal(t, []interface{}{"2", "3", "1"}, buf.rows[0])
	assert.Equal(t, []interface{}{"x", "z", "y"}, buf.rows[1])
}

func TestSheetWriter_BufferFlushThreshold(t *testing.T) {
	mock := &mockSheetOps{}
	batchSize := 5
	w := newSheetWriterWithOps(mock, batchSize)

	// Add exactly batchSize records to trigger auto-flush
	for i := 0; i < batchSize; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"val": i,
		}, "append")
		require.NoError(t, err)
	}

	// Auto-flush should have been triggered
	assert.Len(t, mock.appendedCalls, 2) // header + data
	// Buffer should be cleared
	assert.Empty(t, w.buffers["stream1"].rows)

	// Add one more record (below threshold, no auto-flush)
	err := w.AddRecord("stream1", map[string]interface{}{
		"val": 999,
	}, "append")
	require.NoError(t, err)

	// Still 2 calls (no additional flush)
	assert.Len(t, mock.appendedCalls, 2)
	assert.Len(t, w.buffers["stream1"].rows, 1)
}

func TestSheetWriter_FlushAll(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 100)

	err := w.AddRecord("stream1", map[string]interface{}{"col": "val1"}, "append")
	require.NoError(t, err)
	err = w.AddRecord("stream2", map[string]interface{}{"col": "val2"}, "append")
	require.NoError(t, err)

	err = w.FlushAll()
	require.NoError(t, err)

	// Both streams should have been flushed (header + data each)
	assert.Len(t, mock.ensuredSheets, 2)
	assert.Len(t, mock.appendedCalls, 4) // header+data for each stream
}

func TestSheetWriter_OverwriteMode_ClearOnFirstFlush(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 3)

	// Add 3 records (reaches batchSize, triggers auto-flush)
	for i := 0; i < 3; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"col": i,
		}, "overwrite")
		require.NoError(t, err)
	}

	// clearSheet should have been called exactly once
	assert.Equal(t, []string{"stream1"}, mock.clearedSheets)
	// appendRows: header row + data rows
	assert.Len(t, mock.appendedCalls, 2)

	// First append should be the header
	assert.Equal(t, [][]interface{}{{"col"}}, mock.appendedCalls[0].rows)
	// Second append should be the data
	assert.Len(t, mock.appendedCalls[1].rows, 3)
}

func TestSheetWriter_OverwriteMode_NoClearOnSecondFlush(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 2)

	// First batch (triggers flush at 2 records)
	for i := 0; i < 2; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"col": i,
		}, "overwrite")
		require.NoError(t, err)
	}

	// Clear called once on first flush
	assert.Equal(t, []string{"stream1"}, mock.clearedSheets)
	callsAfterFirst := len(mock.appendedCalls)

	// Second batch (triggers another flush)
	for i := 0; i < 2; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"col": i + 10,
		}, "overwrite")
		require.NoError(t, err)
	}

	// clearSheet should NOT have been called again
	assert.Equal(t, []string{"stream1"}, mock.clearedSheets) // still just one clear
	// But appendRows should have more calls
	assert.Greater(t, len(mock.appendedCalls), callsAfterFirst)
}

func TestSheetWriter_AppendMode_NeverClears(t *testing.T) {
	mock := &mockSheetOps{}
	w := newSheetWriterWithOps(mock, 2)

	// First batch
	for i := 0; i < 2; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"col": i,
		}, "append")
		require.NoError(t, err)
	}

	// clearSheet should never be called in append mode
	assert.Empty(t, mock.clearedSheets)

	// Second batch
	for i := 0; i < 2; i++ {
		err := w.AddRecord("stream1", map[string]interface{}{
			"col": i + 10,
		}, "append")
		require.NoError(t, err)
	}

	// Still no clears
	assert.Empty(t, mock.clearedSheets)
	// appendRows should have been called for header + data batches
	assert.True(t, len(mock.appendedCalls) >= 3) // header + 2 data batches
}
