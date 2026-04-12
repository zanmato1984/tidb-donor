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
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"unsafe"
)

func runTiForthExecutionHostV2TruncateAsWarningDecimalCast(foreignRetainable bool) ([]string, uint32, error) {
	request := C.TiforthExecutionBuildRequestV2{
		abi_version:              C.TIFORTH_EXECUTION_HOST_V2_ABI_VERSION,
		plan_kind:                C.TIFORTH_EXECUTION_PLAN_KIND_CAST_UTF8_TO_DECIMAL,
		ambient_requirement_mask: C.TIFORTH_AMBIENT_REQUIREMENT_SQL_MODE,
		sql_mode:                 C.TIFORTH_EXECUTION_SQL_MODE_TRUNCATE_AS_WARNING,
		session_charset:          C.TIFORTH_EXECUTION_SESSION_CHARSET_NONE,
		default_collation:        C.TIFORTH_EXECUTION_DEFAULT_COLLATION_NONE,
		decimal_precision_is_set: true,
		decimal_precision:        10,
		decimal_scale_is_set:     true,
		decimal_scale:            2,
	}

	var buildStatus C.TiforthStatusV2
	var executable *C.TiforthExecutionExecutableHandleV2
	C.tiforth_execution_host_v2_build(&request, &buildStatus, &executable)
	if err := statusErrorV2("build", &buildStatus); err != nil {
		return nil, 0, err
	}
	defer C.tiforth_execution_host_v2_release_executable(executable)

	var openStatus C.TiforthStatusV2
	var instance *C.TiforthExecutionInstanceHandleV2
	C.tiforth_execution_host_v2_open(executable, &openStatus, &instance)
	if err := statusErrorV2("open", &openStatus); err != nil {
		return nil, 0, err
	}
	defer C.tiforth_execution_host_v2_release_instance(instance)

	offsets := []int32{0, 4, 8, 8}
	data := []byte("5.207.00")
	validity := []byte{1, 1, 0}
	nullBitmap := executionHostV2BitmapFromValidity(validity)

	columnsPtr := (*C.TiforthExecutionColumnViewV2)(C.calloc(1, C.sizeof_TiforthExecutionColumnViewV2))
	defer C.free(unsafe.Pointer(columnsPtr))
	columns := unsafe.Slice(columnsPtr, 1)
	columns[0].physical_type = C.TIFORTH_PHYSICAL_TYPE_UTF8

	var foreignBuffers []unsafe.Pointer
	defer func() {
		for _, ptr := range foreignBuffers {
			C.free(ptr)
		}
	}()

	ownershipMode := C.uint32_t(C.TIFORTH_EXECUTION_BATCH_OWNERSHIP_BORROW_WITHIN_CALL)
	if foreignRetainable {
		ownershipMode = C.TIFORTH_EXECUTION_BATCH_OWNERSHIP_FOREIGN_RETAINABLE
		if ptr := mallocCopy(len(nullBitmap), unsafe.Pointer(unsafe.SliceData(nullBitmap))); ptr != nil {
			foreignBuffers = append(foreignBuffers, ptr)
			columns[0].null_bitmap = (*C.uint8_t)(ptr)
		}
		if ptr := mallocCopy(
			len(offsets)*int(C.sizeof_int32_t),
			unsafe.Pointer(unsafe.SliceData(offsets)),
		); ptr != nil {
			foreignBuffers = append(foreignBuffers, ptr)
			columns[0].offsets = (*C.int32_t)(ptr)
		}
		if ptr := mallocCopy(len(data), unsafe.Pointer(unsafe.SliceData(data))); ptr != nil {
			foreignBuffers = append(foreignBuffers, ptr)
			columns[0].data = (*C.uint8_t)(ptr)
		}
	} else {
		columns[0].null_bitmap = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(nullBitmap)))
		columns[0].offsets = (*C.int32_t)(unsafe.Pointer(unsafe.SliceData(offsets)))
		columns[0].data = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(data)))
	}

	input := C.TiforthBatchViewV2{
		abi_version:    C.TIFORTH_EXECUTION_HOST_V2_ABI_VERSION,
		ownership_mode: ownershipMode,
		column_count:   1,
		row_count:      3,
		columns:        columnsPtr,
	}

	var driveStatus C.TiforthStatusV2
	var output C.TiforthBatchViewV2
	warningCount := uint32(0)
	C.tiforth_execution_host_v2_drive_input_batch(
		instance,
		C.TIFORTH_EXECUTION_INPUT_ID_SCALAR,
		&input,
		&driveStatus,
		&output,
	)
	if err := statusErrorV2("drive_input_batch", &driveStatus); err != nil {
		return nil, 0, err
	}
	warningCount += uint32(driveStatus.warning_count)

	outputColumns := unsafe.Slice((*C.TiforthExecutionColumnViewV2)(unsafe.Pointer(output.columns)), int(output.column_count))
	got := make([]string, int(output.row_count))
	for row := range got {
		if !executionHostV2BitmapIsValid(outputColumns[0].null_bitmap, outputColumns[0].null_bitmap_bit_offset, len(got), row) {
			got[row] = "null"
			continue
		}
		got[row] = executionHostV2Decimal128WordToString(
			unsafe.Pointer(outputColumns[0].decimal128_words),
			int(outputColumns[0].row_offset),
			row,
			int(outputColumns[0].decimal_scale),
		)
	}

	var endStatus C.TiforthStatusV2
	C.tiforth_execution_host_v2_drive_end_of_input(
		instance,
		C.TIFORTH_EXECUTION_INPUT_ID_SCALAR,
		&endStatus,
	)
	if err := statusErrorV2("drive_end_of_input", &endStatus); err != nil {
		return nil, 0, err
	}
	warningCount += uint32(endStatus.warning_count)

	var finishStatus C.TiforthStatusV2
	C.tiforth_execution_host_v2_finish(instance, &finishStatus)
	if err := statusErrorV2("finish", &finishStatus); err != nil {
		return nil, 0, err
	}
	warningCount += uint32(finishStatus.warning_count)

	return got, warningCount, nil
}

// RunHostV2TruncateAsWarningDecimalCast executes the TiForth host-v2 cast
// proving slice and returns row output plus warning count.
func RunHostV2TruncateAsWarningDecimalCast(foreignRetainable bool) ([]string, uint32, error) {
	return runTiForthExecutionHostV2TruncateAsWarningDecimalCast(foreignRetainable)
}

func statusErrorV2(step string, status *C.TiforthStatusV2) error {
	if status.kind == C.TIFORTH_STATUS_KIND_OK {
		return nil
	}
	return fmt.Errorf(
		"%s failed: kind=%d code=%d message=%s",
		step,
		uint32(status.kind),
		uint32(status.code),
		C.GoString(&status.message[0]),
	)
}

func executionHostV2BitmapFromValidity(validity []byte) []byte {
	bitmap := make([]byte, (len(validity)+7)/8)
	for i, v := range validity {
		if v != 0 {
			bitmap[i/8] |= 1 << (i % 8)
		}
	}
	return bitmap
}

func executionHostV2BitmapIsValid(bitmap *C.uint8_t, bitOffset C.uint32_t, rowCount int, row int) bool {
	if bitmap == nil || rowCount == 0 {
		return true
	}
	totalBits := int(bitOffset) + rowCount
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(bitmap)), (totalBits+7)/8)
	bitIndex := int(bitOffset) + row
	return (bytes[bitIndex/8] & (1 << (bitIndex % 8))) != 0
}

func executionHostV2Decimal128WordToString(words unsafe.Pointer, rowOffset int, row int, scale int) string {
	if words == nil {
		return ""
	}
	wordIndex := rowOffset + row
	view := unsafe.Slice((*byte)(words), (wordIndex+1)*16)
	raw := view[wordIndex*16 : (wordIndex+1)*16]
	low := binary.LittleEndian.Uint64(raw[:8])
	high := int64(binary.LittleEndian.Uint64(raw[8:]))
	return decimal128ToString(low, high, scale)
}

func decimal128ToString(low uint64, high int64, scale int) string {
	raw := new(big.Int).Lsh(new(big.Int).SetUint64(uint64(high)), 64)
	raw.Or(raw, new(big.Int).SetUint64(low))
	if high < 0 {
		raw.Sub(raw, new(big.Int).Lsh(big.NewInt(1), 128))
	}

	sign := ""
	if raw.Sign() < 0 {
		sign = "-"
		raw.Abs(raw)
	}

	digits := raw.String()
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	return fmt.Sprintf("%s%s.%s", sign, digits[:split], digits[split:])
}

func mallocCopy(byteLen int, src unsafe.Pointer) unsafe.Pointer {
	if byteLen == 0 || src == nil {
		return nil
	}
	ptr := C.malloc(C.size_t(byteLen))
	if ptr == nil {
		panic("C.malloc failed")
	}
	copy(
		unsafe.Slice((*byte)(ptr), byteLen),
		unsafe.Slice((*byte)(src), byteLen),
	)
	return ptr
}
