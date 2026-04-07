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

/*
#cgo LDFLAGS: -ltiforth_ffi_c
#include <stdlib.h>
#include "tiforth_execution_host_v2.h"
*/
import "C"

import (
	"fmt"
	"sort"
	"unsafe"
)

type nullableString struct {
	Value string
	Valid bool
}

type nullableInt64 struct {
	Value int64
	Valid bool
}

type innerHashJoinInputRow struct {
	JoinKey nullableString
	Payload nullableInt64
}

type innerHashJoinOutputRow struct {
	BuildPayload nullableInt64
	ProbePayload nullableInt64
}

type innerHashJoinRunResult struct {
	Rows         []innerHashJoinOutputRow
	WarningCount uint32
}

func runTiForthExecutionHostV2InnerHashJoinPayload(partitions int, foreignRetainable bool) (innerHashJoinRunResult, error) {
	if partitions <= 0 {
		partitions = 1
	}

	request := C.TiforthExecutionBuildRequestV2{
		abi_version:              C.TIFORTH_EXECUTION_HOST_V2_ABI_VERSION,
		plan_kind:                C.TIFORTH_EXECUTION_PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD,
		ambient_requirement_mask: C.TIFORTH_AMBIENT_REQUIREMENT_CHARSET | C.TIFORTH_AMBIENT_REQUIREMENT_DEFAULT_COLLATION,
		sql_mode:                 0,
		session_charset:          C.TIFORTH_EXECUTION_SESSION_CHARSET_UTF8MB4,
		default_collation:        C.TIFORTH_EXECUTION_DEFAULT_COLLATION_UTF8MB4_BIN,
		decimal_precision_is_set: false,
		decimal_precision:        0,
		decimal_scale_is_set:     false,
		decimal_scale:            0,
		max_block_size:           1,
	}

	var buildStatus C.TiforthStatusV2
	var executable *C.TiforthExecutionExecutableHandleV2
	C.tiforth_execution_host_v2_build(&request, &buildStatus, &executable)
	if err := statusErrorV2("build", &buildStatus); err != nil {
		return innerHashJoinRunResult{}, err
	}
	defer C.tiforth_execution_host_v2_release_executable(executable)

	var openStatus C.TiforthStatusV2
	var instance *C.TiforthExecutionInstanceHandleV2
	C.tiforth_execution_host_v2_open(executable, &openStatus, &instance)
	if err := statusErrorV2("open", &openStatus); err != nil {
		return innerHashJoinRunResult{}, err
	}
	defer C.tiforth_execution_host_v2_release_instance(instance)

	result := innerHashJoinRunResult{Rows: make([]innerHashJoinOutputRow, 0, 8)}

	retainedForeignBuffers := make([]unsafe.Pointer, 0, 16)
	defer freeUnsafePointers(retainedForeignBuffers)

	ownershipMode := C.uint32_t(C.TIFORTH_EXECUTION_BATCH_OWNERSHIP_BORROW_WITHIN_CALL)
	if foreignRetainable {
		ownershipMode = C.TIFORTH_EXECUTION_BATCH_OWNERSHIP_FOREIGN_RETAINABLE
	}

	driveRows := func(rows []innerHashJoinInputRow, inputID C.uint32_t) error {
		chunkSize := (len(rows) + partitions - 1) / partitions
		if chunkSize < 1 {
			chunkSize = 1
		}

		for start := 0; start < len(rows); start += chunkSize {
			end := start + chunkSize
			if end > len(rows) {
				end = len(rows)
			}

			batch, chunkBuffers, columnsPtr, err := buildUTF8Int64JoinBatch(rows[start:end], ownershipMode)
			if err != nil {
				return err
			}

			var driveStatus C.TiforthStatusV2
			var output C.TiforthBatchViewV2
			C.tiforth_execution_host_v2_drive_input_batch(instance, inputID, &batch, &driveStatus, &output)
			C.free(unsafe.Pointer(columnsPtr))

			if foreignRetainable {
				retainedForeignBuffers = append(retainedForeignBuffers, chunkBuffers...)
			} else {
				freeUnsafePointers(chunkBuffers)
			}

			if err := statusErrorV2("drive_input_batch", &driveStatus); err != nil {
				return err
			}
			result.WarningCount += uint32(driveStatus.warning_count)
			if err := appendJoinOutputRowsV2(output, &result.Rows); err != nil {
				return err
			}
			if err := drainContinueOutputRows(instance, &driveStatus, &result); err != nil {
				return err
			}
		}

		return nil
	}

	if err := driveRows(innerHashJoinBuildInputRows(), C.TIFORTH_EXECUTION_INPUT_ID_BUILD); err != nil {
		return innerHashJoinRunResult{}, err
	}

	var buildEndStatus C.TiforthStatusV2
	C.tiforth_execution_host_v2_drive_end_of_input(instance, C.TIFORTH_EXECUTION_INPUT_ID_BUILD, &buildEndStatus)
	if err := statusErrorV2("drive_end_of_input(build)", &buildEndStatus); err != nil {
		return innerHashJoinRunResult{}, err
	}
	if err := drainContinueOutputRows(instance, &buildEndStatus, &result); err != nil {
		return innerHashJoinRunResult{}, err
	}

	if err := driveRows(innerHashJoinProbeInputRows(), C.TIFORTH_EXECUTION_INPUT_ID_PROBE); err != nil {
		return innerHashJoinRunResult{}, err
	}

	var probeEndStatus C.TiforthStatusV2
	C.tiforth_execution_host_v2_drive_end_of_input(instance, C.TIFORTH_EXECUTION_INPUT_ID_PROBE, &probeEndStatus)
	if err := statusErrorV2("drive_end_of_input(probe)", &probeEndStatus); err != nil {
		return innerHashJoinRunResult{}, err
	}
	if err := drainContinueOutputRows(instance, &probeEndStatus, &result); err != nil {
		return innerHashJoinRunResult{}, err
	}

	var finishStatus C.TiforthStatusV2
	C.tiforth_execution_host_v2_finish(instance, &finishStatus)
	if err := statusErrorV2("finish", &finishStatus); err != nil {
		return innerHashJoinRunResult{}, err
	}

	result.Rows = canonicalizeInnerHashJoinRows(result.Rows)
	return result, nil
}

func buildUTF8Int64JoinBatch(
	rows []innerHashJoinInputRow,
	ownershipMode C.uint32_t,
) (C.TiforthBatchViewV2, []unsafe.Pointer, *C.TiforthExecutionColumnViewV2, error) {
	keyNullBitmap := make([]byte, (len(rows)+7)/8)
	keyOffsets := make([]int32, 1, len(rows)+1)
	keyData := make([]byte, 0, len(rows)*4)

	payloadValues := make([]int64, len(rows))
	payloadNullBitmap := make([]byte, (len(rows)+7)/8)

	for i, row := range rows {
		if row.JoinKey.Valid {
			keyNullBitmap[i/8] |= 1 << (i % 8)
			keyData = append(keyData, []byte(row.JoinKey.Value)...)
		}
		keyOffsets = append(keyOffsets, int32(len(keyData)))

		if row.Payload.Valid {
			payloadNullBitmap[i/8] |= 1 << (i % 8)
			payloadValues[i] = row.Payload.Value
		}
	}

	columnsPtr := (*C.TiforthExecutionColumnViewV2)(C.calloc(2, C.sizeof_TiforthExecutionColumnViewV2))
	if columnsPtr == nil {
		return C.TiforthBatchViewV2{}, nil, nil, fmt.Errorf("calloc columns failed")
	}
	columns := unsafe.Slice(columnsPtr, 2)

	buffers := make([]unsafe.Pointer, 0, 5)

	if ptr := mallocCopy(len(keyNullBitmap), unsafeByteSlicePtr(keyNullBitmap)); ptr != nil {
		buffers = append(buffers, ptr)
		columns[0].null_bitmap = (*C.uint8_t)(ptr)
	}
	if ptr := mallocCopy(len(keyOffsets)*int(C.sizeof_int32_t), unsafeInt32SlicePtr(keyOffsets)); ptr != nil {
		buffers = append(buffers, ptr)
		columns[0].offsets = (*C.int32_t)(ptr)
	}
	if ptr := mallocCopy(len(keyData), unsafeByteSlicePtr(keyData)); ptr != nil {
		buffers = append(buffers, ptr)
		columns[0].data = (*C.uint8_t)(ptr)
	}

	columns[0].physical_type = C.TIFORTH_PHYSICAL_TYPE_UTF8
	columns[0].null_bitmap_bit_offset = 0
	columns[0].row_offset = 0

	if ptr := mallocCopy(len(payloadNullBitmap), unsafeByteSlicePtr(payloadNullBitmap)); ptr != nil {
		buffers = append(buffers, ptr)
		columns[1].null_bitmap = (*C.uint8_t)(ptr)
	}
	if ptr := mallocCopy(len(payloadValues)*int(C.sizeof_int64_t), unsafeInt64SlicePtr(payloadValues)); ptr != nil {
		buffers = append(buffers, ptr)
		columns[1].values = (*C.int64_t)(ptr)
	}

	columns[1].physical_type = C.TIFORTH_PHYSICAL_TYPE_INT64
	columns[1].null_bitmap_bit_offset = 0
	columns[1].row_offset = 0

	batch := C.TiforthBatchViewV2{
		abi_version:    C.TIFORTH_EXECUTION_HOST_V2_ABI_VERSION,
		ownership_mode: ownershipMode,
		column_count:   2,
		row_count:      C.uint32_t(len(rows)),
		columns:        columnsPtr,
	}

	return batch, buffers, columnsPtr, nil
}

func drainContinueOutputRows(
	instance *C.TiforthExecutionInstanceHandleV2,
	status *C.TiforthStatusV2,
	result *innerHashJoinRunResult,
) error {
	for status.code == C.TIFORTH_EXECUTION_STATUS_CODE_MORE_OUTPUT_AVAILABLE {
		var continuedOutput C.TiforthBatchViewV2
		C.tiforth_execution_host_v2_continue_output(instance, status, &continuedOutput)
		if err := statusErrorV2("continue_output", status); err != nil {
			return err
		}
		result.WarningCount += uint32(status.warning_count)
		if err := appendJoinOutputRowsV2(continuedOutput, &result.Rows); err != nil {
			return err
		}
	}

	if status.code != C.TIFORTH_STATUS_CODE_NONE {
		return fmt.Errorf("unexpected status code after output drain: %d", uint32(status.code))
	}

	return nil
}

func appendJoinOutputRowsV2(output C.TiforthBatchViewV2, rows *[]innerHashJoinOutputRow) error {
	if output.row_count == 0 {
		return nil
	}
	if output.column_count != 2 {
		return fmt.Errorf("unexpected join output column_count=%d", uint32(output.column_count))
	}

	columns := unsafe.Slice(
		(*C.TiforthExecutionColumnViewV2)(unsafe.Pointer(output.columns)),
		int(output.column_count),
	)
	buildColumn := columns[0]
	probeColumn := columns[1]

	if buildColumn.physical_type != C.TIFORTH_PHYSICAL_TYPE_INT64 {
		return fmt.Errorf("unexpected build payload physical_type=%d", uint32(buildColumn.physical_type))
	}
	if probeColumn.physical_type != C.TIFORTH_PHYSICAL_TYPE_INT64 {
		return fmt.Errorf("unexpected probe payload physical_type=%d", uint32(probeColumn.physical_type))
	}
	if buildColumn.values == nil || probeColumn.values == nil {
		return fmt.Errorf("unexpected nil values pointer in join output")
	}

	rowCount := int(output.row_count)
	buildRowOffset := int(buildColumn.row_offset)
	probeRowOffset := int(probeColumn.row_offset)

	buildValues := unsafe.Slice(buildColumn.values, buildRowOffset+rowCount)
	probeValues := unsafe.Slice(probeColumn.values, probeRowOffset+rowCount)

	for row := 0; row < rowCount; row++ {
		buildPayload := nullableInt64{}
		if executionHostV2BitmapIsValid(buildColumn.null_bitmap, buildColumn.null_bitmap_bit_offset, rowCount, row) {
			buildPayload.Valid = true
			buildPayload.Value = int64(buildValues[buildRowOffset+row])
		}

		probePayload := nullableInt64{}
		if executionHostV2BitmapIsValid(probeColumn.null_bitmap, probeColumn.null_bitmap_bit_offset, rowCount, row) {
			probePayload.Valid = true
			probePayload.Value = int64(probeValues[probeRowOffset+row])
		}

		*rows = append(*rows, innerHashJoinOutputRow{
			BuildPayload: buildPayload,
			ProbePayload: probePayload,
		})
	}

	return nil
}

func canonicalizeInnerHashJoinRows(rows []innerHashJoinOutputRow) []innerHashJoinOutputRow {
	out := append([]innerHashJoinOutputRow(nil), rows...)
	sort.Slice(out, func(i, j int) bool {
		buildCmp := compareNullableInt64(out[i].BuildPayload, out[j].BuildPayload)
		if buildCmp != 0 {
			return buildCmp < 0
		}
		return compareNullableInt64(out[i].ProbePayload, out[j].ProbePayload) < 0
	})
	return out
}

func compareNullableInt64(left, right nullableInt64) int {
	if left.Valid != right.Valid {
		if !left.Valid {
			return -1
		}
		return 1
	}
	if !left.Valid {
		return 0
	}
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	return 0
}

func freeUnsafePointers(buffers []unsafe.Pointer) {
	for _, ptr := range buffers {
		C.free(ptr)
	}
}

func unsafeByteSlicePtr(data []byte) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.SliceData(data))
}

func unsafeInt32SlicePtr(data []int32) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.SliceData(data))
}

func unsafeInt64SlicePtr(data []int64) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(unsafe.SliceData(data))
}

func innerHashJoinBuildInputRows() []innerHashJoinInputRow {
	return []innerHashJoinInputRow{
		{JoinKey: nullableString{Value: "k", Valid: true}, Payload: nullableInt64{Value: 10, Valid: true}},
		{JoinKey: nullableString{Value: "k", Valid: true}, Payload: nullableInt64{Value: 20, Valid: true}},
		{JoinKey: nullableString{Value: "x", Valid: true}, Payload: nullableInt64{Value: 30, Valid: true}},
		{JoinKey: nullableString{Valid: false}, Payload: nullableInt64{Value: 40, Valid: true}},
	}
}

func innerHashJoinProbeInputRows() []innerHashJoinInputRow {
	return []innerHashJoinInputRow{
		{JoinKey: nullableString{Value: "k", Valid: true}, Payload: nullableInt64{Value: 100, Valid: true}},
		{JoinKey: nullableString{Value: "x", Valid: true}, Payload: nullableInt64{Value: 200, Valid: true}},
		{JoinKey: nullableString{Value: "z", Valid: true}, Payload: nullableInt64{Value: 300, Valid: true}},
		{JoinKey: nullableString{Valid: false}, Payload: nullableInt64{Value: 400, Valid: true}},
	}
}
