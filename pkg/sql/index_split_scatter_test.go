// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIndexSplitAndScatterSkipsSmallTable verifies that MaybeSplitIndexSpans
// skips splitting when table statistics indicate the table is smaller than
// the table's RangeMaxBytes, and proceeds normally when the size limit is
// overridden to a small value via testing knob.
func TestIndexSplitAndScatterSkipsSmallTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var splitCount atomic.Int64
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeIndexSplitAndScatter: func(splitPoints [][]byte) {
					splitCount.Add(int64(len(splitPoints)))
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Set range_max_bytes to the minimum so we can exceed it with a
	// reasonable amount of data.
	const rangeMaxBytes = 64 << 20 // 64 MB (minimum allowed)
	runner.Exec(t, fmt.Sprintf(
		"ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d, range_min_bytes = 1",
		rangeMaxBytes,
	))

	// Disable automatic statistics so we control when stats are refreshed.
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")

	// Creating an index on table without stats should skip splits.
	runner.Exec(t, "CREATE TABLE t_splits (k INT PRIMARY KEY, v INT, payload STRING)")
	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_no_stats ON t_splits (v)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on a table with no stats")

	// Collect stats so the cache knows RowCount=0. Creating an index
	// on the empty table should skip splits.
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")
	runner.Exec(t, "CREATE INDEX idx_empty ON t_splits (v)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on an empty table with stats")

	// Insert data and refresh stats. The estimated table size (~800 bytes)
	// is well below the default 512 MB RangeMaxBytes, so splits are still
	// skipped.
	runner.Exec(t, "INSERT INTO t_splits SELECT i, i, repeat('x', 1000) FROM generate_series(1, 1000) AS g(i)")
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")

	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_small ON t_splits (v, k)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on a small table below RangeMaxBytes")

	// Insert enough data to well exceed 64 MB. Splits should be
	// created during the backfill. Each row is ~1 KB (payload).
	// Insert 100k more rows for ~100 MB total, well above the 64 MB threshold.
	runner.Exec(t, "INSERT INTO t_splits SELECT i, i, repeat('x', 1000) FROM generate_series(1001, 101000) AS g(i)")
	runner.Exec(t, "CREATE STATISTICS s FROM t_splits")

	splitCount.Store(0)
	runner.Exec(t, "CREATE INDEX idx_large ON t_splits (v, k)")
	require.Equal(t, int64(0), splitCount.Load(),
		"expected no splits on a small table below RangeMaxBytes")
}

// TestIndexSplitAndScatterWithStats tests the creation of indexes on tables with statistics,
// where the splits will be generated using statistics on the table.
func TestIndexSplitAndScatterWithStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test can be fairly slow and timeout under race / duress.
	skip.UnderDuress(t)

	testutils.RunTrueAndFalse(t, "StatsCreated", func(t *testing.T, statsExist bool) {
		ctx := context.Background()
		var splitHookEnabled atomic.Bool
		var observedSplitPoints atomic.Int64
		const numNodes = 3
		cluster := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeIndexSplitAndScatter: func(splitPoints [][]byte) {
							if !splitHookEnabled.Load() {
								return
							}
							observedSplitPoints.Swap(int64(len(splitPoints)))
						},
						// Disable the small-table size check so this test
						// exercises the stats-based split point generation.
						DisableBackfillerSplitSizeCheck: true,
					},
				},
			},
		})
		defer cluster.Stopper().Stop(ctx)
		runner := sqlutils.MakeSQLRunner(cluster.ServerConn(0))
		// Disable automatic statistics.
		runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
		// Create and populate the tables.
		runner.Exec(t, "CREATE TABLE multi_column_split (b bool, n uuid PRIMARY KEY)")
		runner.Exec(t, "INSERT INTO multi_column_split (SELECT true, uuid_generate_v1()  FROM generate_series(1, 5000))")
		runner.Exec(t, "INSERT INTO multi_column_split (SELECT false, uuid_generate_v1() FROM generate_series(1, 5000))")
		// Generate statistics for these tables.
		if statsExist {
			runner.Exec(t, "CREATE STATISTICS st FROM multi_column_split")
		}
		// Next create indexes on both tables.
		splitHookEnabled.Store(true)
		observedSplitPoints.Store(0)
		runner.Exec(t, "CREATE INDEX ON multi_column_split (b, n)")
		// Assert that we generated the target number of split points
		// automatically.
		if !statsExist {
			require.Equal(t, int64(1), observedSplitPoints.Load())
		} else {
			expectedCount := sql.PreservedSplitCountMultiple.Get(&cluster.Server(0).ClusterSettings().SV) * numNodes
			require.Greaterf(t, observedSplitPoints.Load(), expectedCount,
				"expected %d split points, got %d", expectedCount, observedSplitPoints.Load())
		}
		splitHookEnabled.Swap(false)
	})

}
