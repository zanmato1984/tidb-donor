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

package hashjoin

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/tiforthadapter"
	"github.com/stretchr/testify/require"
)

func TestTiForthHostV2CastDonorNativeEntryParitySerialAndParallel(t *testing.T) {
	wantRows, wantWarnings := runDonorNativeCastBaseline(t)

	t.Run("serial", func(t *testing.T) {
		borrowRows, borrowWarnings, err := tiforthadapter.RunHostV2TruncateAsWarningDecimalCast(false)
		require.NoError(t, err)
		require.Equal(t, wantWarnings, borrowWarnings)
		require.Equal(t, wantRows, borrowRows)

		foreignRows, foreignWarnings, err := tiforthadapter.RunHostV2TruncateAsWarningDecimalCast(true)
		require.NoError(t, err)
		require.Equal(t, wantWarnings, foreignWarnings)
		require.Equal(t, wantRows, foreignRows)
	})

	t.Run("parallel", func(t *testing.T) {
		runParallelParity(
			t,
			4,
			func(worker int) ([]string, uint32, error) {
				foreignRetainable := worker%2 == 1
				return tiforthadapter.RunHostV2TruncateAsWarningDecimalCast(foreignRetainable)
			},
			wantRows,
			wantWarnings,
		)
	})
}

func TestTiForthHostV2InnerHashJoinDonorNativeEntryParitySerialAndParallel(t *testing.T) {
	wantRows, wantWarnings := runDonorNativeInnerHashJoinBaseline(t)

	t.Run("serial", func(t *testing.T) {
		borrowRows, borrowWarnings, err := tiforthadapter.RunHostV2InnerHashJoinPayloadRows(1, false)
		require.NoError(t, err)
		require.Equal(t, wantWarnings, borrowWarnings)
		require.Equal(t, wantRows, borrowRows)

		foreignRows, foreignWarnings, err := tiforthadapter.RunHostV2InnerHashJoinPayloadRows(1, true)
		require.NoError(t, err)
		require.Equal(t, wantWarnings, foreignWarnings)
		require.Equal(t, wantRows, foreignRows)

		highPartitionRows, highPartitionWarnings, err := tiforthadapter.RunHostV2InnerHashJoinPayloadRows(8, true)
		require.NoError(t, err)
		require.Equal(t, wantWarnings, highPartitionWarnings)
		require.Equal(t, wantRows, highPartitionRows)
	})

	t.Run("parallel", func(t *testing.T) {
		runParallelParity(
			t,
			4,
			func(worker int) ([]string, uint32, error) {
				foreignRetainable := worker%2 == 1
				partitions := 2
				if foreignRetainable {
					partitions = 8
				}
				return tiforthadapter.RunHostV2InnerHashJoinPayloadRows(partitions, foreignRetainable)
			},
			wantRows,
			wantWarnings,
		)
	})
}

func runParallelParity(
	t *testing.T,
	workers int,
	run func(worker int) ([]string, uint32, error),
	wantRows []string,
	wantWarnings uint32,
) {
	t.Helper()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			rows, warnings, err := run(worker)
			if err != nil {
				errCh <- fmt.Errorf("worker %d run failed: %w", worker, err)
				return
			}
			if warnings != wantWarnings {
				errCh <- fmt.Errorf(
					"worker %d warning mismatch: got=%d want=%d",
					worker,
					warnings,
					wantWarnings,
				)
				return
			}
			if len(rows) != len(wantRows) {
				errCh <- fmt.Errorf(
					"worker %d row count mismatch: got=%d want=%d",
					worker,
					len(rows),
					len(wantRows),
				)
				return
			}
			for idx := range rows {
				if rows[idx] != wantRows[idx] {
					errCh <- fmt.Errorf(
						"worker %d row %d mismatch: got=%q want=%q",
						worker,
						idx,
						rows[idx],
						wantRows[idx],
					)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func runDonorNativeCastBaseline(t *testing.T) ([]string, uint32) {
	t.Helper()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")

	rows := tk.MustQuery(`
		select cast(v as decimal(10,2))
		from (
			select 1 as id, '1.239' as v
			union all select 2, '5.20'
			union all select 3, null
		) as src
		order by id
	`).Rows()

	warnings := uint32(len(tk.MustQuery("show warnings").Rows()))
	require.Greater(t, warnings, uint32(0))
	return formatQueryRows(rows), warnings
}

func runDonorNativeInnerHashJoinBaseline(t *testing.T) ([]string, uint32) {
	t.Helper()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	rows := tk.MustQuery(`
		select b.payload, p.payload
		from (
			select 'k' as join_key, 10 as payload
			union all select 'k', 20
			union all select 'x', 30
			union all select null, 40
		) as b
		join (
			select 'k' as join_key, 100 as payload
			union all select 'x', 200
			union all select 'z', 300
			union all select null, 400
		) as p
		on b.join_key = p.join_key
		order by b.payload, p.payload
	`).Rows()

	warnings := uint32(len(tk.MustQuery("show warnings").Rows()))
	return formatQueryRows(rows), warnings
}

func formatQueryRows(rows [][]any) []string {
	formatted := make([]string, len(rows))
	for i, row := range rows {
		if len(row) == 0 {
			formatted[i] = ""
			continue
		}

		text := normalizeQueryValue(row[0])
		for _, col := range row[1:] {
			text += " " + normalizeQueryValue(col)
		}
		formatted[i] = text
	}
	return formatted
}

func normalizeQueryValue(value any) string {
	if value == nil {
		return "null"
	}
	switch v := value.(type) {
	case []byte:
		if string(v) == "<nil>" {
			return "null"
		}
		return string(v)
	case string:
		if v == "<nil>" {
			return "null"
		}
		return v
	default:
		return fmt.Sprint(v)
	}
}
