package oracle

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

func newTestQuotaManager(nodeID string, srcs []sources.Source) (*quotaManager, *[]*OracleSnapshot) {
	var updates []*OracleSnapshot
	return newQuotaManager(&quotaManagerConfig{
		log:     slog.Disabled,
		nodeID:  nodeID,
		sources: srcs,
		publishQuotaHeartbeat: func(_ context.Context, _ map[string]*sources.QuotaStatus) error {
			return nil
		},
		onStateUpdate: func(snap *OracleSnapshot) {
			updates = append(updates, snap)
		},
	}), &updates
}

func makeQuota(remaining, limit int64, resetIn time.Duration) *sources.QuotaStatus {
	return &sources.QuotaStatus{
		FetchesRemaining: remaining,
		FetchesLimit:     limit,
		ResetTime:        time.Now().Add(resetIn),
	}
}

func makeTimestampedQuota(remaining, limit int64, resetIn time.Duration, receivedAt time.Time) *TimestampedQuotaStatus {
	return &TimestampedQuotaStatus{
		QuotaStatus: makeQuota(remaining, limit, resetIn),
		ReceivedAt:  receivedAt,
	}
}

// --- computeNetworkSchedule tests (pure function, no quotaManager) ---

func TestComputeNetworkScheduleSingleNode(t *testing.T) {
	now := time.Now()
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": makeTimestampedQuota(100, 200, 24*time.Hour, now),
	}

	sched := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)

	if len(sched.OrderedNodes) != 1 {
		t.Fatalf("expected 1 ordered node, got %d", len(sched.OrderedNodes))
	}
	if sched.OrderedNodes[0] != "node-A" {
		t.Errorf("expected node-A, got %s", sched.OrderedNodes[0])
	}
	if sched.MinPeriod != 30*time.Second {
		t.Errorf("expected min period 30s, got %v", sched.MinPeriod)
	}
	if sched.NetworkSustainableRate <= 0 {
		t.Error("expected positive sustainable rate")
	}
	// Single node at position 0: no propagation delay.
	expectedNext := sched.NetworkNextFetchTime
	if sched.NextFetchTime != expectedNext {
		t.Errorf("single node should have no propagation delay, got diff %v",
			sched.NextFetchTime.Sub(expectedNext))
	}
}

func TestComputeNetworkScheduleDeterministicOrder(t *testing.T) {
	now := time.Now()
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": makeTimestampedQuota(100, 200, 24*time.Hour, now),
		"node-B": makeTimestampedQuota(100, 200, 24*time.Hour, now),
	}

	sched1 := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)
	sched2 := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)

	if len(sched1.OrderedNodes) != 2 {
		t.Fatalf("expected 2 ordered nodes, got %d", len(sched1.OrderedNodes))
	}
	for i := range sched1.OrderedNodes {
		if sched1.OrderedNodes[i] != sched2.OrderedNodes[i] {
			t.Error("expected deterministic ordering across calls")
			break
		}
	}
}

func TestComputeNetworkScheduleConsistentAcrossNodes(t *testing.T) {
	now := time.Now()
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": makeTimestampedQuota(100, 200, 24*time.Hour, now),
		"node-B": makeTimestampedQuota(100, 200, 24*time.Hour, now),
		"node-C": makeTimestampedQuota(100, 200, 24*time.Hour, now),
	}

	// Different nodes calling with the same peer set should produce the same ordering.
	schedA := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)
	schedB := computeNetworkSchedule(peers, "node-B", 30*time.Second, now)

	for i := range schedA.OrderedNodes {
		if schedA.OrderedNodes[i] != schedB.OrderedNodes[i] {
			t.Error("ordering should be the same regardless of which node computes it")
			break
		}
	}
}

func TestComputeNetworkScheduleRespectsMinPeriod(t *testing.T) {
	now := time.Now()
	// Unlimited quota — sustainable period would be 1s (rate=1.0), far below minPeriod.
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": {
			QuotaStatus: &sources.QuotaStatus{
				FetchesRemaining: 1 << 62,
				FetchesLimit:     1 << 62,
				ResetTime:        now.Add(24 * time.Hour),
			},
			ReceivedAt: now,
		},
	}

	sched := computeNetworkSchedule(peers, "node-A", 5*time.Minute, now)

	expectedMin := now.Add(5*time.Minute - time.Second)
	if sched.NetworkNextFetchTime.Before(expectedMin) {
		t.Error("network next fetch time should respect min period")
	}
}

func TestComputeNetworkScheduleRespectsMaxPeriod(t *testing.T) {
	now := time.Now()
	// Exhausted quota — rate is 0, sustainable period would be infinite.
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": makeTimestampedQuota(0, 100, 24*time.Hour, now),
	}

	sched := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)

	if sched.NetworkSustainablePeriod != maxPeriod {
		t.Errorf("expected sustainable period capped at maxPeriod (%v), got %v",
			maxPeriod, sched.NetworkSustainablePeriod)
	}
}

func TestComputeNetworkSchedulePropagationDelay(t *testing.T) {
	now := time.Now()
	peers := map[string]*TimestampedQuotaStatus{
		"node-A": makeTimestampedQuota(100, 200, 24*time.Hour, now),
		"node-B": makeTimestampedQuota(100, 200, 24*time.Hour, now),
	}

	sched := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)

	myOrder := -1
	for i, id := range sched.OrderedNodes {
		if id == "node-A" {
			myOrder = i
			break
		}
	}
	if myOrder < 0 {
		t.Fatal("local node not found in ordered nodes")
	}

	expectedDelay := time.Duration(myOrder) * propagationDelay
	diff := sched.NextFetchTime.Sub(sched.NetworkNextFetchTime)
	if diff < expectedDelay-time.Millisecond || diff > expectedDelay+time.Millisecond {
		t.Errorf("expected propagation delay of %v for order %d, got %v", expectedDelay, myOrder, diff)
	}
}

func TestComputeNetworkScheduleHigherRateBiasesOrder(t *testing.T) {
	now := time.Now()
	// Give node-A much more remaining quota than node-B.
	// Over many time windows, node-A should appear first more often.
	aFirst := 0
	trials := 100
	for i := range trials {
		trialTime := now.Add(time.Duration(i) * time.Hour)
		peers := map[string]*TimestampedQuotaStatus{
			"node-A": {
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 10000,
					FetchesLimit:     10000,
					ResetTime:        trialTime.Add(24 * time.Hour),
				},
				ReceivedAt: trialTime,
			},
			"node-B": {
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 10,
					FetchesLimit:     10000,
					ResetTime:        trialTime.Add(24 * time.Hour),
				},
				ReceivedAt: trialTime,
			},
		}
		sched := computeNetworkSchedule(peers, "node-A", 30*time.Second, trialTime)
		if sched.OrderedNodes[0] == "node-A" {
			aFirst++
		}
	}
	// node-A has ~1000x the rate, so it should be first most of the time.
	if aFirst < trials/2 {
		t.Errorf("node with higher rate should be ordered first more often, but was first only %d/%d times", aFirst, trials)
	}
}

func TestComputeNetworkScheduleNoPeers(t *testing.T) {
	now := time.Now()
	peers := map[string]*TimestampedQuotaStatus{}

	sched := computeNetworkSchedule(peers, "node-A", 30*time.Second, now)

	if len(sched.OrderedNodes) != 0 {
		t.Errorf("expected 0 ordered nodes with no peers, got %d", len(sched.OrderedNodes))
	}
	if sched.NetworkSustainablePeriod != maxPeriod {
		t.Errorf("expected maxPeriod with no peers, got %v", sched.NetworkSustainablePeriod)
	}
}

func TestComputeNetworkScheduleNetworkRate(t *testing.T) {
	now := time.Now()
	resetTime := now.Add(time.Hour)

	single := map[string]*TimestampedQuotaStatus{
		"node-A": {
			QuotaStatus: &sources.QuotaStatus{
				FetchesRemaining: 1000,
				FetchesLimit:     1000,
				ResetTime:        resetTime,
			},
			ReceivedAt: now,
		},
	}
	double := map[string]*TimestampedQuotaStatus{
		"node-A": {
			QuotaStatus: &sources.QuotaStatus{
				FetchesRemaining: 1000,
				FetchesLimit:     1000,
				ResetTime:        resetTime,
			},
			ReceivedAt: now,
		},
		"node-B": {
			QuotaStatus: &sources.QuotaStatus{
				FetchesRemaining: 1000,
				FetchesLimit:     1000,
				ResetTime:        resetTime,
			},
			ReceivedAt: now,
		},
	}

	sched1 := computeNetworkSchedule(single, "node-A", time.Second, now)
	sched2 := computeNetworkSchedule(double, "node-A", time.Second, now)

	// Two identical peers should have ~2x the network rate.
	ratio := sched2.NetworkSustainableRate / sched1.NetworkSustainableRate
	if math.Abs(ratio-2.0) > 0.01 {
		t.Errorf("expected 2x network rate with 2 peers, got ratio %.2f", ratio)
	}
}

// --- sustainableRate tests ---

func TestSustainableRate(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		quota    *TimestampedQuotaStatus
		wantRate float64
		wantZero bool
	}{
		{
			name: "unlimited quota returns capped rate",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 1 << 62,
					FetchesLimit:     1 << 62,
					ResetTime:        now.Add(24 * time.Hour),
				},
			},
			wantRate: 1.0,
		},
		{
			name: "exhausted quota returns zero",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 0,
					FetchesLimit:     100,
					ResetTime:        now.Add(time.Hour),
				},
			},
			wantZero: true,
		},
		{
			name: "negative remaining returns zero",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: -5,
					FetchesLimit:     100,
					ResetTime:        now.Add(time.Hour),
				},
			},
			wantZero: true,
		},
		{
			name: "expired reset time returns capped rate",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 50,
					FetchesLimit:     100,
					ResetTime:        now.Add(-time.Hour),
				},
			},
			wantRate: 1.0,
		},
		{
			name: "normal quota calculates rate with safety margin",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 1000,
					FetchesLimit:     1000,
					ResetTime:        now.Add(time.Hour),
				},
			},
			// effective = 1000 * 0.9 = 900, time = 3600s, rate = 0.25
			wantRate: 900.0 / 3600.0,
		},
		{
			name: "very low remaining with margin",
			quota: &TimestampedQuotaStatus{
				QuotaStatus: &sources.QuotaStatus{
					FetchesRemaining: 1,
					FetchesLimit:     100,
					ResetTime:        now.Add(time.Hour),
				},
			},
			// effective = 1 * 0.9 = 0.9, time = 3600s
			wantRate: 0.9 / 3600.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rate := sustainableRate(tt.quota, now)
			if tt.wantZero {
				if rate != 0 {
					t.Errorf("expected 0, got %f", rate)
				}
				return
			}
			if math.Abs(rate-tt.wantRate) > 1e-9 {
				t.Errorf("expected %f, got %f", tt.wantRate, rate)
			}
		})
	}
}

// --- quotaManager tests (external interface only) ---

func TestQuotaManagerHandlePeerQuota(t *testing.T) {
	src := &mockSource{
		name:  "blockcypher",
		quota: makeQuota(100, 200, 24*time.Hour),
	}
	qm, updates := newTestQuotaManager("node-A", []sources.Source{src})

	qm.handlePeerSourceQuota("node-B", &TimestampedQuotaStatus{
		QuotaStatus: makeQuota(50, 200, 12*time.Hour),
		ReceivedAt:  time.Now(),
	}, "blockcypher")

	// Should emit a state update.
	if len(*updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(*updates))
	}
	snap := (*updates)[0]
	q, ok := snap.Sources["blockcypher"].Quotas["node-B"]
	if !ok {
		t.Fatal("expected node-B quota in snapshot")
	}
	if q.FetchesRemaining != 50 || q.FetchesLimit != 200 {
		t.Errorf("unexpected quota values: remaining=%d, limit=%d", q.FetchesRemaining, q.FetchesLimit)
	}

	// Should be stored in network quotas.
	peers := qm.getNetworkQuotas()
	if _, ok := peers["node-B"]["blockcypher"]; !ok {
		t.Error("expected peer quota stored for node-B/blockcypher")
	}
}

func TestQuotaManagerHandlePeerQuotaOverwrite(t *testing.T) {
	qm, _ := newTestQuotaManager("node-A", nil)
	now := time.Now()

	qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(100, 200, 12*time.Hour, now), "blockcypher")
	qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(50, 200, 12*time.Hour, now.Add(time.Minute)), "blockcypher")

	peers := qm.getNetworkQuotas()
	if peers["node-B"]["blockcypher"].FetchesRemaining != 50 {
		t.Errorf("expected overwritten quota with 50 remaining, got %d",
			peers["node-B"]["blockcypher"].FetchesRemaining)
	}
}

func TestQuotaManagerGetLocalQuotas(t *testing.T) {
	qm, _ := newTestQuotaManager("node-A", []sources.Source{
		&mockSource{name: "blockcypher", quota: makeQuota(80, 200, 24*time.Hour)},
		&mockSource{name: "coinpaprika", quota: makeQuota(500, 1000, 12*time.Hour)},
	})

	quotas := qm.getLocalQuotas()
	if len(quotas) != 2 {
		t.Fatalf("expected 2 local quotas, got %d", len(quotas))
	}
	if quotas["blockcypher"].FetchesRemaining != 80 {
		t.Errorf("expected 80 remaining for blockcypher, got %d", quotas["blockcypher"].FetchesRemaining)
	}
	if quotas["coinpaprika"].FetchesRemaining != 500 {
		t.Errorf("expected 500 remaining for coinpaprika, got %d", quotas["coinpaprika"].FetchesRemaining)
	}
}

func TestQuotaManagerGetNetworkQuotasMapIndependence(t *testing.T) {
	qm, _ := newTestQuotaManager("node-A", nil)
	qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(50, 100, time.Hour, time.Now()), "blockcypher")

	copy1 := qm.getNetworkQuotas()
	delete(copy1, "node-B")

	copy2 := qm.getNetworkQuotas()
	if _, ok := copy2["node-B"]; !ok {
		t.Error("deleting from returned map should not affect internal state")
	}
}

func TestQuotaManagerRunContextCancellation(t *testing.T) {
	qm, _ := newTestQuotaManager("node-A", nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		qm.run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestQuotaManagerRunHeartbeatAndExpiration(t *testing.T) {
	src := &mockSource{name: "blockcypher", quota: makeQuota(100, 200, 24*time.Hour)}
	var publishedQuotas map[string]*sources.QuotaStatus

	qm := newQuotaManager(&quotaManagerConfig{
		log:     slog.Disabled,
		nodeID:  "node-A",
		sources: []sources.Source{src},
		publishQuotaHeartbeat: func(_ context.Context, quotas map[string]*sources.QuotaStatus) error {
			publishedQuotas = quotas
			return nil
		},
		onStateUpdate: func(_ *OracleSnapshot) {},
	})

	// Add a stale peer.
	qm.peerQuotasMtx.Lock()
	qm.peerQuotas["stale-peer"] = map[string]*TimestampedQuotaStatus{
		"blockcypher": makeTimestampedQuota(10, 100, time.Hour, time.Now().Add(-quotaPeerActiveThreshold-time.Minute)),
	}
	qm.peerQuotasMtx.Unlock()

	// Simulate what run() does each tick.
	ctx := context.Background()
	if err := qm.publishHeartbeat(ctx, qm.getLocalQuotas()); err != nil {
		t.Fatalf("publishHeartbeat failed: %v", err)
	}
	qm.expireStalePeerQuotas()

	if _, ok := publishedQuotas["blockcypher"]; !ok {
		t.Error("expected blockcypher quota in heartbeat")
	}
	if _, ok := qm.getNetworkQuotas()["stale-peer"]; ok {
		t.Error("expected stale peer to be expired")
	}
}

// --- quotaManager integration tests ---

func TestGetActivePeersForSource(t *testing.T) {
	src := &mockSource{
		name:      "dcrdata",
		weight:    1.0,
		minPeriod: 30 * time.Second,
	}

	qm, _ := newTestQuotaManager("node-A", []sources.Source{src})
	now := time.Now()

	t.Run("includes local node", func(t *testing.T) {
		peers := qm.getActivePeersForSource("dcrdata", now)
		if _, ok := peers["node-A"]; !ok {
			t.Fatal("expected local node in active peers")
		}
		if peers["node-A"].QuotaStatus == nil {
			t.Error("expected quota status for local node")
		}
	})

	t.Run("filters inactive peers by 6-minute threshold", func(t *testing.T) {
		// Add active peer (1 minute old)
		recentTime := now.Add(-1 * time.Minute)
		qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(500, 1000, 24*time.Hour, recentTime), "dcrdata")

		// Add stale peer (10 minutes old)
		staleTime := now.Add(-10 * time.Minute)
		qm.handlePeerSourceQuota("node-C", makeTimestampedQuota(500, 1000, 24*time.Hour, staleTime), "dcrdata")

		peers := qm.getActivePeersForSource("dcrdata", now)

		if _, ok := peers["node-B"]; !ok {
			t.Error("expected active peer node-B")
		}
		if _, ok := peers["node-C"]; ok {
			t.Error("expected stale peer node-C to be filtered")
		}
	})

	t.Run("returns only relevant source", func(t *testing.T) {
		coingecko := &mockSource{
			name:      "coingecko",
			weight:    1.0,
			minPeriod: 30 * time.Second,
		}

		qm2, _ := newTestQuotaManager("node-X", []sources.Source{coingecko})

		qm2.handlePeerSourceQuota("node-Y", makeTimestampedQuota(100, 200, 24*time.Hour, now), "coingecko")
		qm2.handlePeerSourceQuota("node-Y", makeTimestampedQuota(300, 400, 24*time.Hour, now), "dcrdata")

		// Query for coingecko
		peers := qm2.getActivePeersForSource("coingecko", now)

		if len(peers) != 2 { // local + node-Y
			t.Errorf("expected 2 peers, got %d", len(peers))
		}
		// Verify correct quota for coingecko
		if peers["node-Y"].FetchesRemaining != 100 {
			t.Errorf("expected coingecko quota 100, got %d", peers["node-Y"].FetchesRemaining)
		}
	})

	t.Run("returns empty for nonexistent source", func(t *testing.T) {
		peers := qm.getActivePeersForSource("nonexistent", now)
		if len(peers) != 0 {
			t.Errorf("expected 0 peers, got %d", len(peers))
		}
	})
}

func TestGetNetworkSchedule(t *testing.T) {
	src := &mockSource{
		name:      "dcrdata",
		weight:    1.0,
		minPeriod: 30 * time.Second,
	}

	qm, _ := newTestQuotaManager("node-A", []sources.Source{src})
	now := time.Now()

	t.Run("single node includes self in order", func(t *testing.T) {
		sched := qm.getNetworkSchedule("dcrdata", 30*time.Second)

		if len(sched.OrderedNodes) != 1 {
			t.Errorf("expected 1 node, got %d", len(sched.OrderedNodes))
		}
		if sched.OrderedNodes[0] != "node-A" {
			t.Errorf("expected node-A, got %s", sched.OrderedNodes[0])
		}
	})

	t.Run("respects minimum period", func(t *testing.T) {
		sched := qm.getNetworkSchedule("dcrdata", 30*time.Second)

		if sched.MinPeriod < 30*time.Second {
			t.Errorf("expected min period >= 30s, got %v", sched.MinPeriod)
		}
	})

	t.Run("multi-node includes active peers", func(t *testing.T) {
		qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(500, 1000, 24*time.Hour, now), "dcrdata")
		qm.handlePeerSourceQuota("node-C", makeTimestampedQuota(500, 1000, 24*time.Hour, now), "dcrdata")

		sched := qm.getNetworkSchedule("dcrdata", 30*time.Second)

		if len(sched.OrderedNodes) != 3 {
			t.Errorf("expected 3 nodes, got %d", len(sched.OrderedNodes))
		}
	})

	t.Run("computes network sustainable rate", func(t *testing.T) {
		qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(500, 1000, 24*time.Hour, now), "dcrdata")

		sched := qm.getNetworkSchedule("dcrdata", 30*time.Second)

		if sched.NetworkSustainableRate <= 0 {
			t.Errorf("expected positive network rate, got %v", sched.NetworkSustainableRate)
		}
	})

	t.Run("deterministic ordering across calls", func(t *testing.T) {
		qm.handlePeerSourceQuota("node-B", makeTimestampedQuota(500, 1000, 24*time.Hour, now), "dcrdata")
		qm.handlePeerSourceQuota("node-C", makeTimestampedQuota(500, 1000, 24*time.Hour, now), "dcrdata")

		sched1 := qm.getNetworkSchedule("dcrdata", 30*time.Second)
		sched2 := qm.getNetworkSchedule("dcrdata", 30*time.Second)

		if len(sched1.OrderedNodes) != len(sched2.OrderedNodes) {
			t.Fatal("ordering changed between calls")
		}

		for i, node := range sched1.OrderedNodes {
			if node != sched2.OrderedNodes[i] {
				t.Errorf("node at position %d changed: %s -> %s", i, node, sched2.OrderedNodes[i])
			}
		}
	})
}
