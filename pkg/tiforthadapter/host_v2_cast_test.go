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
	"time"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestTiForthExecutionHostV2TruncateAsWarningDecimalCastParitySerial(t *testing.T) {
	nativeOutput, nativeWarnings := runTiDBNativeTruncateAsWarningDecimalCast(t)

	borrowOutput, borrowWarnings, err := runTiForthExecutionHostV2TruncateAsWarningDecimalCast(false)
	require.NoError(t, err)
	require.Equal(t, nativeWarnings, borrowWarnings)
	require.Equal(t, nativeOutput, borrowOutput)

	foreignOutput, foreignWarnings, err := runTiForthExecutionHostV2TruncateAsWarningDecimalCast(true)
	require.NoError(t, err)
	require.Equal(t, nativeWarnings, foreignWarnings)
	require.Equal(t, nativeOutput, foreignOutput)
}

func TestTiForthExecutionHostV2TruncateAsWarningDecimalCastParityParallel(t *testing.T) {
	nativeOutput, nativeWarnings := runTiDBNativeTruncateAsWarningDecimalCast(t)

	const workers = 2
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			foreignRetainable := worker%2 == 1

			output, warnings, err := runTiForthExecutionHostV2TruncateAsWarningDecimalCast(foreignRetainable)
			if err != nil {
				errs <- fmt.Errorf("worker %d host-v2 run failed: %w", worker, err)
				return
			}
			if warnings != nativeWarnings {
				errs <- fmt.Errorf(
					"worker %d warning mismatch: got=%d want=%d",
					worker,
					warnings,
					nativeWarnings,
				)
				return
			}
			if len(output) != len(nativeOutput) {
				errs <- fmt.Errorf(
					"worker %d output length mismatch: got=%d want=%d",
					worker,
					len(output),
					len(nativeOutput),
				)
				return
			}
			for i := range output {
				if output[i] != nativeOutput[i] {
					errs <- fmt.Errorf(
						"worker %d row %d mismatch: got=%q want=%q",
						worker,
						i,
						output[i],
						nativeOutput[i],
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

func runTiDBNativeTruncateAsWarningDecimalCast(t *testing.T) ([]string, uint32) {
	warnings := contextutil.NewStaticWarnHandler(0)
	ctx := types.NewContext(
		types.DefaultStmtFlags.WithTruncateAsWarning(true),
		time.UTC,
		warnings,
	)

	targetType := types.NewFieldTypeBuilder().
		SetType(mysql.TypeNewDecimal).
		SetFlag(mysql.BinaryFlag).
		SetCharset(charset.CharsetBin).
		SetCollate(charset.CollationBin).
		SetFlen(10).
		SetDecimal(2).
		BuildP()

	inputs := []*string{toPtr("5.20"), toPtr("7.00"), nil}
	outputs := make([]string, len(inputs))
	for i, input := range inputs {
		if input == nil {
			outputs[i] = "null"
			continue
		}

		dec, err := types.ConvertDatumToDecimal(ctx, types.NewStringDatum(*input))
		require.NoError(t, err)

		dec, err = types.ProduceDecWithSpecifiedTp(ctx, dec, targetType)
		require.NoError(t, err)
		outputs[i] = dec.String()
	}

	return outputs, uint32(warnings.WarningCount())
}

func toPtr(s string) *string {
	return &s
}
