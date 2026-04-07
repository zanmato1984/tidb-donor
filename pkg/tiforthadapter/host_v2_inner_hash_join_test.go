// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build tiforth_adapter && cgo

package tiforthadapter

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestTiForthExecutionHostV2InnerHashJoinPayloadParitySerial(t *testing.T) {
	nativeRows, nativeWarnings := runTiDBNativeInnerHashJoinPayload()

	borrowResult, err := runTiForthExecutionHostV2InnerHashJoinPayload(1, false)
	require.NoError(t, err)
	require.Equal(t, nativeWarnings, borrowResult.WarningCount)
	require.Equal(t, nativeRows, borrowResult.Rows)

	foreignResult, err := runTiForthExecutionHostV2InnerHashJoinPayload(1, true)
	require.NoError(t, err)
	require.Equal(t, nativeWarnings, foreignResult.WarningCount)
	require.Equal(t, nativeRows, foreignResult.Rows)
}

func TestTiForthExecutionHostV2InnerHashJoinPayloadParityParallel(t *testing.T) {
	nativeRows, nativeWarnings := runTiDBNativeInnerHashJoinPayload()

	const workers = 2
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			foreignRetainable := worker%2 == 1
			result, err := runTiForthExecutionHostV2InnerHashJoinPayload(2, foreignRetainable)
			if err != nil {
				errs <- fmt.Errorf("worker %d host-v2 inner-join run failed: %w", worker, err)
				return
			}
			if result.WarningCount != nativeWarnings {
				errs <- fmt.Errorf(
					"worker %d warning mismatch: got=%d want=%d",
					worker,
					result.WarningCount,
					nativeWarnings,
				)
				return
			}
			if len(result.Rows) != len(nativeRows) {
				errs <- fmt.Errorf(
					"worker %d output length mismatch: got=%d want=%d",
					worker,
					len(result.Rows),
					len(nativeRows),
				)
				return
			}
			for i := range result.Rows {
				if result.Rows[i] != nativeRows[i] {
					errs <- fmt.Errorf(
						"worker %d row %d mismatch: got=%+v want=%+v",
						worker,
						i,
						result.Rows[i],
						nativeRows[i],
					)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestTiForthExecutionHostV2InnerHashJoinPayloadForeignRetainableReleasesRetainedBuffers(t *testing.T) {
	originalFreeUnsafePointers := freeUnsafePointersFn
	t.Cleanup(func() {
		freeUnsafePointersFn = originalFreeUnsafePointers
	})

	var releasedBufferCount int
	freeUnsafePointersFn = func(buffers []unsafe.Pointer) {
		releasedBufferCount += len(buffers)
		originalFreeUnsafePointers(buffers)
	}

	_, err := runTiForthExecutionHostV2InnerHashJoinPayload(2, true)
	require.NoError(t, err)
	require.Greater(t, releasedBufferCount, 0)
}

func runTiDBNativeInnerHashJoinPayload() ([]innerHashJoinOutputRow, uint32) {
	buildRows := innerHashJoinBuildInputRows()
	probeRows := innerHashJoinProbeInputRows()

	result := make([]innerHashJoinOutputRow, 0, 4)
	for _, probeRow := range probeRows {
		if !probeRow.JoinKey.Valid {
			continue
		}
		for _, buildRow := range buildRows {
			if !buildRow.JoinKey.Valid {
				continue
			}
			if buildRow.JoinKey.Value != probeRow.JoinKey.Value {
				continue
			}
			result = append(result, innerHashJoinOutputRow{
				BuildPayload: buildRow.Payload,
				ProbePayload: probeRow.Payload,
			})
		}
	}

	return canonicalizeInnerHashJoinRows(result), 0
}
