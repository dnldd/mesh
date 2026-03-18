package oracle

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

// makePriceBuckets converts a test-friendly format to the Oracle's bucket format.
func makePriceBuckets(m map[Ticker]map[string]*priceUpdate) map[Ticker]*priceBucket {
	result := make(map[Ticker]*priceBucket, len(m))
	for ticker, sources := range m {
		bucket := newPriceBucket()
		for source, update := range sources {
			bucket.mergeAndUpdateAggregate(source, update)
		}
		result[ticker] = bucket
	}
	return result
}

// makeFeeRateBuckets converts a test-friendly format to the Oracle's bucket format.
func makeFeeRateBuckets(m map[Network]map[string]*feeRateUpdate) map[Network]*feeRateBucket {
	result := make(map[Network]*feeRateBucket, len(m))
	for network, sources := range m {
		bucket := newFeeRateBucket()
		for source, update := range sources {
			bucket.mergeAndUpdateAggregate(source, update)
		}
		result[network] = bucket
	}
	return result
}

func newTestOracle(log slog.Logger) *Oracle {
	qm := newQuotaManager(&quotaManagerConfig{
		log:                   log,
		nodeID:                "test-node",
		publishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		onStateUpdate:         func(*OracleSnapshot) {},
		sources:               []sources.Source{},
	})
	return &Oracle{
		log:           log,
		prices:        make(map[Ticker]*priceBucket),
		feeRates:      make(map[Network]*feeRateBucket),
		diviners:      make(map[string]*diviner),
		fetchTracker:  newFetchTracker(log),
		quotaManager:  qm,
		onStateUpdate: func(*OracleSnapshot) {},
	}
}

func setSourceWeights(oracle *Oracle, weights map[string]float64) {
	for name, weight := range weights {
		oracle.diviners[name] = &diviner{source: &mockSource{name: name, weight: weight}}
	}
}

func TestMergePrices(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name           string
		existingPrices map[Ticker]map[string]*priceUpdate
		update         *OracleUpdate
		sourceWeights  map[string]float64
		expectedPrices map[Ticker]map[string]*priceUpdate
		expectedResult map[Ticker]float64
	}{
		{
			name:           "new ticker from external source",
			existingPrices: map[Ticker]map[string]*priceUpdate{},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 50000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with newer timestamp should update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  48000.0,
						stamp:  oldStamp,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				Prices: map[Ticker]float64{
					"BTC": 50000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with older timestamp should ignore",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				Prices: map[Ticker]float64{
					"BTC": 48000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple prices in single update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 51000.0,
					"ETH": 3000.0,
				},
			},
			sourceWeights: map[string]float64{
				"source2": 0.8,
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
				"ETH": {
					"source2": {
						ticker: "ETH",
						price:  3000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50444.444444444445253, // (50000*1.0 + 51000*0.8) / 1.8
				"ETH": 3000.0,
			},
		},
		{
			name: "new source for existing ticker",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 51000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50500.0, // (50000*1.0 + 51000*1.0) / 2.0
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := newTestOracle(log)
			oracle.prices = makePriceBuckets(tt.existingPrices)
			if len(tt.sourceWeights) > 0 {
				setSourceWeights(oracle, tt.sourceWeights)
			}

			mergeResult := oracle.Merge(tt.update, "test-sender")

			// Extract price results
			var result map[Ticker]float64
			if mergeResult != nil {
				result = mergeResult.Prices
			}

			// Verify the merged prices match expected
			if len(oracle.prices) != len(tt.expectedPrices) {
				t.Errorf("Expected %d tickers, got %d", len(tt.expectedPrices), len(oracle.prices))
			}

			for ticker, expectedSources := range tt.expectedPrices {
				actualBucket, found := oracle.prices[ticker]
				if !found {
					t.Errorf("Expected ticker %s to be in oracle.prices", ticker)
					continue
				}

				if len(actualBucket.sources) != len(expectedSources) {
					t.Errorf("For ticker %s, expected %d sources, got %d",
						ticker, len(expectedSources), len(actualBucket.sources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualBucket.sources[source]
					if !found {
						t.Errorf("Expected source %s for ticker %s", source, ticker)
						continue
					}

					if actualUpdate.price != expectedUpdate.price {
						t.Errorf("For ticker %s source %s, expected price %.2f, got %.2f",
							ticker, source, expectedUpdate.price, actualUpdate.price)
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For ticker %s source %s, expected weight %.2f, got %.2f",
							ticker, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For ticker %s source %s, expected stamp %v, got %v",
							ticker, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d tickers in result, got %d", len(tt.expectedResult), len(result))
					}
					for ticker, expectedPrice := range tt.expectedResult {
						actualPrice, found := result[ticker]
						if !found {
							t.Errorf("Expected ticker %s in result", ticker)
							continue
						}
						if actualPrice != expectedPrice {
							t.Errorf("For ticker %s, expected aggregated price %.15f, got %.15f",
								ticker, expectedPrice, actualPrice)
						}
					}
					for ticker := range result {
						if _, expected := tt.expectedResult[ticker]; !expected {
							t.Errorf("Unexpected ticker %s in result", ticker)
						}
					}
				}
			}
		})
	}
}

func TestMergePrices_SkipsNegativeAndZeroPrices(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name           string
		existingPrices map[Ticker]map[string]*priceUpdate
		update         *OracleUpdate
		expectedResult map[Ticker]float64
	}{
		{
			name: "negative price is skipped",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": -1000.0, // Negative price - should be skipped
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0, // Only source1's price is used
			},
		},
		{
			name: "zero price is skipped",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"ETH": {
					"source1": {
						ticker: "ETH",
						price:  3000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"ETH": 0.0, // Zero price - should be skipped
				},
			},
			expectedResult: map[Ticker]float64{
				"ETH": 3000.0, // Only source1's price is used
			},
		},
		{
			name: "all prices invalid results in no update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"XYZ": {
					"source1": {
						ticker: "XYZ",
						price:  -100.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"XYZ": 0.0,
				},
			},
			expectedResult: nil, // No valid prices, nothing in result
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := newTestOracle(log)
			oracle.prices = makePriceBuckets(tt.existingPrices)

			mergeResult := oracle.Merge(tt.update, "test-sender")

			if tt.expectedResult == nil {
				if mergeResult != nil && len(mergeResult.Prices) > 0 {
					t.Errorf("Expected no prices in result, got %v", mergeResult.Prices)
				}
				return
			}

			if mergeResult == nil || mergeResult.Prices == nil {
				t.Errorf("Expected prices in result, got nil")
				return
			}

			for ticker, expectedPrice := range tt.expectedResult {
				actualPrice, found := mergeResult.Prices[ticker]
				if !found {
					t.Errorf("Expected ticker %s in result", ticker)
					continue
				}
				if actualPrice != expectedPrice {
					t.Errorf("For ticker %s, expected price %.2f, got %.2f",
						ticker, expectedPrice, actualPrice)
				}
			}
		})
	}
}

func TestMergeFeeRates(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name             string
		existingFeeRates map[Network]map[string]*feeRateUpdate
		update           *OracleUpdate
		sourceWeights    map[string]float64
		expectedFeeRates map[Network]map[string]*feeRateUpdate
		expectedResult   map[Network]*big.Int
	}{
		{
			name:             "new network from external source",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(100),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with newer timestamp should update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(80),
						stamp:   oldStamp,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(100),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with older timestamp should ignore",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(80),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple fee rates in single update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(120),
					"ETH": big.NewInt(50),
				},
			},
			sourceWeights: map[string]float64{
				"source2": 0.8,
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  0.8,
					},
				},
				"ETH": {
					"source2": {
						network: "ETH",
						feeRate: big.NewInt(50),
						stamp:   now,
						weight:  0.8,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(109), // round((100*1.0 + 120*0.8) / 1.8) = round(108.888...) = 109
				"ETH": big.NewInt(50),
			},
		},
		{
			name: "new source for existing network",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(120),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(110), // round((100*1.0 + 120*1.0) / 2.0) = round(110.0) = 110
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := newTestOracle(log)
			oracle.feeRates = makeFeeRateBuckets(tt.existingFeeRates)
			if len(tt.sourceWeights) > 0 {
				setSourceWeights(oracle, tt.sourceWeights)
			}

			mergeResult := oracle.Merge(tt.update, "test-sender")

			// Extract fee rate results
			var result map[Network]*big.Int
			if mergeResult != nil {
				result = mergeResult.FeeRates
			}

			// Verify the merged fee rates match expected
			if len(oracle.feeRates) != len(tt.expectedFeeRates) {
				t.Errorf("Expected %d networks, got %d", len(tt.expectedFeeRates), len(oracle.feeRates))
			}

			for network, expectedSources := range tt.expectedFeeRates {
				actualBucket, found := oracle.feeRates[network]
				if !found {
					t.Errorf("Expected network %s to be in oracle.feeRates", network)
					continue
				}

				if len(actualBucket.sources) != len(expectedSources) {
					t.Errorf("For network %s, expected %d sources, got %d",
						network, len(expectedSources), len(actualBucket.sources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualBucket.sources[source]
					if !found {
						t.Errorf("Expected source %s for network %s", source, network)
						continue
					}

					if actualUpdate.feeRate.Cmp(expectedUpdate.feeRate) != 0 {
						t.Errorf("For network %s source %s, expected fee rate %s, got %s",
							network, source, expectedUpdate.feeRate.String(), actualUpdate.feeRate.String())
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For network %s source %s, expected weight %.2f, got %.2f",
							network, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For network %s source %s, expected stamp %v, got %v",
							network, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d networks in result, got %d", len(tt.expectedResult), len(result))
					}
					for network, expectedRate := range tt.expectedResult {
						actualRate, found := result[network]
						if !found {
							t.Errorf("Expected network %s in result", network)
							continue
						}
						if actualRate.Cmp(expectedRate) != 0 {
							t.Errorf("For network %s, expected aggregated fee rate %s, got %s",
								network, expectedRate.String(), actualRate.String())
						}
					}
					for network := range result {
						if _, expected := tt.expectedResult[network]; !expected {
							t.Errorf("Unexpected network %s in result", network)
						}
					}
				}
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("multiple goroutines reading prices simultaneously", func(t *testing.T) {
		now := time.Now()
		oracle := newTestOracle(log)
		oracle.prices = makePriceBuckets(map[Ticker]map[string]*priceUpdate{
			"BTC": {
				"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
				"source2": {ticker: "BTC", price: 51000.0, stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
			},
		})

		// Launch multiple readers concurrently
		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					prices := oracle.allPrices()
					if len(prices) > 0 {
						// Verify data integrity
						if btcPrice, found := prices["BTC"]; found {
							if btcPrice < 0 {
								t.Errorf("Invalid BTC price: %.2f", btcPrice)
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("multiple goroutines reading fee rates simultaneously", func(t *testing.T) {
		now := time.Now()
		oracle := newTestOracle(log)
		oracle.feeRates = makeFeeRateBuckets(map[Network]map[string]*feeRateUpdate{
			"BTC": {
				"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
				"source2": {network: "BTC", feeRate: big.NewInt(120), stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
			},
		})

		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					feeRates := oracle.allFeeRates()
					if len(feeRates) > 0 {
						// Verify data integrity
						if btcRate, found := feeRates["BTC"]; found {
							if btcRate.Sign() == 0 {
								t.Error("Invalid BTC fee rate: 0")
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of prices", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allPrices()
					_, _ = oracle.Price("BTC")
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Prices: map[Ticker]float64{
							"BTC": float64(50000 + j),
							"ETH": float64(3000 + j),
						},
					}
					oracle.Merge(update, fmt.Sprintf("writer-%d", writerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of fee rates", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allFeeRates()
					_, _ = oracle.FeeRate("BTC")
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						FeeRates: map[Network]*big.Int{
							"BTC": big.NewInt(int64(100 + j)),
							"ETH": big.NewInt(int64(50 + j)),
						},
					}
					oracle.Merge(update, fmt.Sprintf("writer-%d", writerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent merge and read operations", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numMergers = 10
		done := make(chan bool, numReaders+numMergers)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allPrices()
					_ = oracle.allFeeRates()
				}
				done <- true
			}()
		}

		// Launch mergers
		for i := 0; i < numMergers; i++ {
			mergerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("merger-%d", mergerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Prices: map[Ticker]float64{
							"BTC": float64(50000 + j),
						},
						FeeRates: map[Network]*big.Int{
							"BTC": big.NewInt(int64(100 + j)),
						},
					}
					oracle.Merge(update, fmt.Sprintf("merger-%d", mergerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numMergers; i++ {
			<-done
		}
	})
}

func TestPublicPrices(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all prices", func(t *testing.T) {
		oracle := newTestOracle(log)
		oracle.prices = makePriceBuckets(map[Ticker]map[string]*priceUpdate{
			"BTC": {
				"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
			},
		})

		result := oracle.allPrices()

		if len(result) != 2 {
			t.Errorf("Expected 2 prices, got %d", len(result))
		}

		if result["BTC"] != 50000.0 {
			t.Errorf("Expected BTC price 50000.0, got %.2f", result["BTC"])
		}

		if result["ETH"] != 3000.0 {
			t.Errorf("Expected ETH price 3000.0, got %.2f", result["ETH"])
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.allPrices()

		if len(result) != 0 {
			t.Errorf("Expected 0 prices, got %d", len(result))
		}
	})
}

func TestPublicFeeRates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all fee rates", func(t *testing.T) {
		oracle := newTestOracle(log)
		oracle.feeRates = makeFeeRateBuckets(map[Network]map[string]*feeRateUpdate{
			"BTC": {
				"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
			},
		})

		result := oracle.allFeeRates()

		if len(result) != 2 {
			t.Errorf("Expected 2 fee rates, got %d", len(result))
		}

		if result["BTC"].Cmp(big.NewInt(100)) != 0 {
			t.Errorf("Expected BTC fee rate 100, got %s", result["BTC"].String())
		}

		if result["ETH"].Cmp(big.NewInt(50)) != 0 {
			t.Errorf("Expected ETH fee rate 50, got %s", result["ETH"].String())
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.allFeeRates()

		if len(result) != 0 {
			t.Errorf("Expected 0 fee rates, got %d", len(result))
		}
	})
}

func TestMergeWithEmptyUpdates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("Merge with nil", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		result := oracle.Merge(nil, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}

		if len(oracle.prices) != 0 {
			t.Errorf("Expected no prices, got %d", len(oracle.prices))
		}
	})

	t.Run("Merge with empty prices map", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.Merge(&OracleUpdate{
			Source: "test",
			Prices: map[Ticker]float64{},
		}, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})

	t.Run("Merge with empty fee rates map", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.Merge(&OracleUpdate{
			Source:   "test",
			FeeRates: map[Network]*big.Int{},
		}, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})
}

// TestAgedWeightBoundaries tests edge cases in weight calculation.
func TestAgedWeightBoundaries(t *testing.T) {
	const defaultWeight = 1.0

	t.Run("exactly at full validity period boundary", func(t *testing.T) {
		stamp := time.Now().Add(-fullValidityPeriod)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly fullValidityPeriod, should still be full weight
		if weight < 0.99 || weight > 1.0 {
			t.Errorf("Expected weight ~1.0 at fullValidityPeriod boundary, got %.4f", weight)
		}
	})

	t.Run("exactly at expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly validityExpiration, should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 at expiration boundary, got %.4f", weight)
		}
	})

	t.Run("just before expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration + time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// Just before expiration should be very small but not zero
		if weight <= 0 || weight > 0.01 {
			t.Errorf("Expected small positive weight just before expiration, got %.4f", weight)
		}
	})

	t.Run("just after expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration - time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// After expiration should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 after expiration, got %.4f", weight)
		}
	})

	t.Run("future timestamp", func(t *testing.T) {
		futureStamp := time.Now().Add(time.Hour)
		weight := agedWeight(defaultWeight, futureStamp)

		// Future timestamps should get full weight (age is negative)
		if weight != defaultWeight {
			t.Errorf("Expected full weight for future timestamp, got %.4f", weight)
		}
	})

	t.Run("very old timestamp", func(t *testing.T) {
		veryOld := time.Now().Add(-24 * time.Hour)
		weight := agedWeight(defaultWeight, veryOld)

		// Very old should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 for very old timestamp, got %.4f", weight)
		}
	})

	t.Run("zero default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0, stamp)

		// Zero weight should always be zero
		if weight != 0 {
			t.Errorf("Expected weight 0 for zero default weight, got %.4f", weight)
		}
	})

	t.Run("fractional default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0.5, stamp)

		// Fresh timestamp with 0.5 weight should return 0.5
		if weight != 0.5 {
			t.Errorf("Expected weight 0.5 for fresh timestamp with 0.5 default, got %.4f", weight)
		}
	})

	t.Run("decay progression is exponential", func(t *testing.T) {
		// Test that decay is exponential with 1-minute halflives
		oneHalflife := time.Now().Add(-fullValidityPeriod - exponentialDecayHalfLife)
		twoHalflives := time.Now().Add(-fullValidityPeriod - 2*exponentialDecayHalfLife)
		threeHalflives := time.Now().Add(-fullValidityPeriod - 3*exponentialDecayHalfLife)

		weight1 := agedWeight(defaultWeight, oneHalflife)
		weight2 := agedWeight(defaultWeight, twoHalflives)
		weight3 := agedWeight(defaultWeight, threeHalflives)

		// Exponential decay: 0.5, 0.25, 0.125
		if weight1 < 0.45 || weight1 > 0.55 {
			t.Errorf("Expected weight ~0.5 at 1 halflife, got %.4f", weight1)
		}
		if weight2 < 0.20 || weight2 > 0.30 {
			t.Errorf("Expected weight ~0.25 at 2 halflives, got %.4f", weight2)
		}
		if weight3 < 0.10 || weight3 > 0.15 {
			t.Errorf("Expected weight ~0.125 at 3 halflives, got %.4f", weight3)
		}

		// Verify progression is decreasing exponentially
		if weight1 <= weight2 || weight2 <= weight3 {
			t.Errorf("Weight should decrease exponentially: %.4f > %.4f > %.4f",
				weight1, weight2, weight3)
		}
	})

	t.Run("within full validity period", func(t *testing.T) {
		// Test various points within full validity period (0-1 minute)
		fresh := time.Now()
		halfMinute := time.Now().Add(-30 * time.Second)
		almostFull := time.Now().Add(-900 * time.Millisecond)

		weights := []float64{
			agedWeight(defaultWeight, fresh),
			agedWeight(defaultWeight, halfMinute),
			agedWeight(defaultWeight, almostFull),
		}

		// All should be full weight since fullValidityPeriod is 1 minute
		for i, w := range weights {
			if w != defaultWeight {
				t.Errorf("Expected full weight at point %d, got %.4f", i, w)
			}
		}

		// Test that weight starts decaying after fullValidityPeriod
		oneMinuteTen := time.Now().Add(-(time.Minute + 10*time.Second))
		weightAfter := agedWeight(defaultWeight, oneMinuteTen)
		if weightAfter >= defaultWeight {
			t.Errorf("Expected weight to decay after fullValidityPeriod, got %.4f", weightAfter)
		}
	})
}

func TestRescheduleDiviner(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("reschedules existing diviner", func(t *testing.T) {
		mockDiv := &diviner{
			source:     &mockSource{name: "test-source"},
			resetTimer: make(chan struct{}, 1),
		}

		oracle := newTestOracle(log)
		oracle.diviners = map[string]*diviner{
			"test-source": mockDiv,
		}

		oracle.rescheduleDiviner("test-source", "other-node")

		// Verify the reschedule signal was sent
		select {
		case <-mockDiv.resetTimer:
			// Success - signal was sent
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected reschedule signal to be sent")
		}
	})

	t.Run("does nothing for non-existent diviner", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		oracle.rescheduleDiviner("non-existent", "other-node")
	})

	t.Run("does nothing when diviners is empty", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		oracle.rescheduleDiviner("any-source", "other-node")
	})
}

func TestRun(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("Run completes with no diviners", func(t *testing.T) {
		qm := newQuotaManager(&quotaManagerConfig{
			log:    log,
			nodeID: "test-node",
		})
		oracle := newTestOracle(log)
		oracle.diviners = make(map[string]*diviner)
		oracle.quotaManager = qm

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		// Cancel immediately since there are no diviners
		cancel()

		select {
		case <-done:
			// Success - Run exited after cancel
		case <-time.After(time.Second):
			t.Error("Run did not complete after context cancellation")
		}
	})

	t.Run("Run waits for diviners and exits on context cancellation", func(t *testing.T) {
		qm := newQuotaManager(&quotaManagerConfig{
			log:    log,
			nodeID: "test-node",
		})

		// Create mock diviners that wait for context
		mockDiviners := make(map[string]*diviner)
		for i := 0; i < 2; i++ {
			name := fmt.Sprintf("source%d", i)
			localName := name
			mockDiviners[name] = &diviner{
				source: &mockSource{
					name:      name,
					minPeriod: time.Hour, // Long period to avoid immediate fetch
					fetchFunc: func(ctx context.Context) (*sources.RateInfo, error) {
						<-ctx.Done() // Block until context cancelled
						return nil, ctx.Err()
					},
				},
				resetTimer: make(chan struct{}),
				log:        log,
				getNetworkSchedule: func() networkSchedule {
					now := time.Now()
					activePeers := qm.getActivePeersForSource(localName, now)
					return computeNetworkSchedule(activePeers, "local", time.Hour, now)
				},
				onScheduleChanged: func(*OracleSnapshot) {},
			}
		}

		oracle := newTestOracle(log)
		oracle.diviners = mockDiviners
		oracle.quotaManager = qm

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		// Give Run time to start goroutines
		time.Sleep(50 * time.Millisecond)

		// Cancel and wait for completion
		cancel()

		select {
		case <-done:
			// Success - Run exited
		case <-time.After(time.Second):
			t.Error("Run did not exit after context cancellation")
		}
	})
}

func TestNewOracle(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("creates oracle with default sources", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle == nil {
			t.Fatal("Expected non-nil oracle")
		}

		if oracle.log != log {
			t.Error("Expected logger to be set")
		}

		if len(oracle.diviners) == 0 {
			t.Error("Expected diviners to be initialized with default sources")
		}

		if oracle.prices == nil || oracle.feeRates == nil {
			t.Error("Expected prices and fee rates maps to be initialized")
		}
	})

	t.Run("initializes with unauthed sources", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify some default unauthed sources exist
		if len(oracle.diviners) == 0 {
			t.Error("Expected at least some diviners from unauthed sources")
		}
	})

	t.Run("nil http client uses default client", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			HTTPClient:            nil,
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient == nil {
			t.Error("Expected httpClient to be set to default")
		}
	})

	t.Run("custom http client is used", func(t *testing.T) {
		customClient := &mockHTTPClient{}
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			HTTPClient:            customClient,
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient != customClient {
			t.Error("Expected custom HTTP client to be used")
		}
	})

	t.Run("initializes empty price and fee rate maps", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(oracle.prices) != 0 {
			t.Error("Expected empty prices map")
		}

		if len(oracle.feeRates) != 0 {
			t.Error("Expected empty fee rates map")
		}
	})
}

// mockHTTPClient is a mock implementation of HTTPClient for testing
type mockHTTPClient struct{}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return nil, nil
}

// TestOracleSnapshot_Empty tests snapshot with no data.
func TestOracleSnapshot_Empty(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	snapshot := oracle.OracleSnapshot()

	if snapshot == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if snapshot.NodeID != "node1" {
		t.Errorf("got node_id %q, want %q", snapshot.NodeID, "node1")
	}
	if len(snapshot.Prices) != 0 {
		t.Errorf("got %d prices, want 0", len(snapshot.Prices))
	}
	if len(snapshot.FeeRates) != 0 {
		t.Errorf("got %d fee rates, want 0", len(snapshot.FeeRates))
	}
	if len(snapshot.Sources) != 0 {
		t.Errorf("got %d sources, want 0", len(snapshot.Sources))
	}
}

// TestOracleSnapshot_WithPrices tests snapshot with populated prices.
func TestOracleSnapshot_WithPrices(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	now := time.Now()
	setSourceWeights(oracle, map[string]float64{
		"source1": 1.0,
		"source2": 0.5,
	})

	// Populate prices
	oracle.prices["BTC"] = newPriceBucket()
	oracle.prices["BTC"].mergeAndUpdateAggregate("source1", &priceUpdate{
		ticker: "BTC",
		price:  50000.0,
		stamp:  now,
		weight: 1.0,
	})
	oracle.prices["BTC"].mergeAndUpdateAggregate("source2", &priceUpdate{
		ticker: "BTC",
		price:  51000.0,
		stamp:  now,
		weight: 0.5,
	})

	oracle.prices["ETH"] = newPriceBucket()
	oracle.prices["ETH"].mergeAndUpdateAggregate("source1", &priceUpdate{
		ticker: "ETH",
		price:  3000.0,
		stamp:  now,
		weight: 1.0,
	})

	snapshot := oracle.OracleSnapshot()

	// Verify prices structure
	if len(snapshot.Prices) != 2 {
		t.Errorf("got %d prices, want 2", len(snapshot.Prices))
	}

	// Verify BTC price exists
	btcRate, exists := snapshot.Prices["BTC"]
	if !exists {
		t.Fatal("BTC price not found in snapshot")
	}
	if btcRate == nil {
		t.Fatal("BTC SnapshotRate is nil")
	}

	// Verify contributions for BTC
	if len(btcRate.Contributions) != 2 {
		t.Errorf("BTC: got %d contributions, want 2", len(btcRate.Contributions))
	}
	if _, hasSource1 := btcRate.Contributions["source1"]; !hasSource1 {
		t.Error("BTC: source1 contribution missing")
	}
	if _, hasSource2 := btcRate.Contributions["source2"]; !hasSource2 {
		t.Error("BTC: source2 contribution missing")
	}

	// Verify ETH price
	ethRate, exists := snapshot.Prices["ETH"]
	if !exists {
		t.Fatal("ETH price not found in snapshot")
	}
	if len(ethRate.Contributions) != 1 {
		t.Errorf("ETH: got %d contributions, want 1", len(ethRate.Contributions))
	}
}

// TestOracleSnapshot_WithFeeRates tests snapshot with populated fee rates.
func TestOracleSnapshot_WithFeeRates(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	now := time.Now()
	setSourceWeights(oracle, map[string]float64{
		"source1": 1.0,
	})

	// Populate fee rates
	oracle.feeRates["BTC"] = newFeeRateBucket()
	oracle.feeRates["BTC"].mergeAndUpdateAggregate("source1", &feeRateUpdate{
		network: "BTC",
		feeRate: big.NewInt(50),
		stamp:   now,
		weight:  1.0,
	})

	oracle.feeRates["ETH"] = newFeeRateBucket()
	oracle.feeRates["ETH"].mergeAndUpdateAggregate("source1", &feeRateUpdate{
		network: "ETH",
		feeRate: big.NewInt(30),
		stamp:   now,
		weight:  1.0,
	})

	snapshot := oracle.OracleSnapshot()

	// Verify fee rates structure
	if len(snapshot.FeeRates) != 2 {
		t.Errorf("got %d fee rates, want 2", len(snapshot.FeeRates))
	}

	// Verify BTC fee rate exists
	btcRate, exists := snapshot.FeeRates["BTC"]
	if !exists {
		t.Fatal("BTC fee rate not found in snapshot")
	}
	if btcRate == nil {
		t.Fatal("BTC SnapshotRate is nil")
	}
	if btcRate.Value != "50" {
		t.Errorf("BTC: got value %q, want %q", btcRate.Value, "50")
	}

	// Verify ETH also included
	ethRate, exists := snapshot.FeeRates["ETH"]
	if !exists {
		t.Fatal("ETH fee rate not found in snapshot")
	}
	if ethRate.Value != "30" {
		t.Errorf("ETH: got value %q, want %q", ethRate.Value, "30")
	}
}

// TestOracleSnapshot_WithMultipleSources tests snapshot with multiple sources.
func TestOracleSnapshot_WithMultipleSources(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	now := time.Now()
	setSourceWeights(oracle, map[string]float64{
		"coingecko":   1.0,
		"coinpaprika": 1.0,
		"dcrdata":     1.0,
	})

	// Add prices from multiple sources
	oracle.prices["BTC"] = newPriceBucket()
	for _, source := range []string{"coingecko", "coinpaprika", "dcrdata"} {
		oracle.prices["BTC"].mergeAndUpdateAggregate(source, &priceUpdate{
			ticker: "BTC",
			price:  50000.0,
			stamp:  now,
			weight: 1.0,
		})
	}

	snapshot := oracle.OracleSnapshot()

	btcRate := snapshot.Prices["BTC"]
	if btcRate == nil {
		t.Fatal("BTC rate is nil")
	}
	if len(btcRate.Contributions) != 3 {
		t.Errorf("got %d contributions, want 3", len(btcRate.Contributions))
	}

	// Verify all sources are present
	for _, source := range []string{"coingecko", "coinpaprika", "dcrdata"} {
		if _, exists := btcRate.Contributions[source]; !exists {
			t.Errorf("source %q missing from contributions", source)
		}
	}
}

// TestOracleSnapshot_ConcurrentAccess tests that OracleSnapshot is safe with concurrent Merge operations.
func TestOracleSnapshot_ConcurrentAccess(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	now := time.Now()
	setSourceWeights(oracle, map[string]float64{
		"source1": 1.0,
		"source2": 1.0,
		"source3": 1.0,
	})

	done := make(chan bool)

	// Goroutine 1: Repeatedly merge prices
	go func() {
		for i := 0; i < 100; i++ {
			update := &OracleUpdate{
				Source: "source1",
				Stamp:  now.Add(time.Duration(i) * time.Millisecond),
				Prices: map[Ticker]float64{
					"BTC": 50000.0 + float64(i),
					"ETH": 3000.0 + float64(i),
				},
			}
			oracle.Merge(update, "node1")
		}
		done <- true
	}()

	// Goroutine 2: Repeatedly take snapshots
	go func() {
		for i := 0; i < 100; i++ {
			_ = oracle.OracleSnapshot()
		}
		done <- true
	}()

	// Goroutine 3: Merge fee rates
	go func() {
		for i := 0; i < 50; i++ {
			update := &OracleUpdate{
				Source: "source2",
				Stamp:  now.Add(time.Duration(i*2) * time.Millisecond),
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(int64(50 + i)),
					"ETH": big.NewInt(int64(10 + i)),
				},
			}
			oracle.Merge(update, "node1")
		}
		done <- true
	}()

	// Wait for all goroutines
	<-done
	<-done
	<-done

	// Verify final snapshot is valid
	snapshot := oracle.OracleSnapshot()
	if snapshot == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if len(snapshot.Prices) == 0 && len(snapshot.FeeRates) == 0 {
		t.Fatal("expected some prices or fee rates in final snapshot")
	}
}

// TestPriceContributions tests price contribution aggregation.
func TestPriceContributions(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)

	now := time.Now()

	// Add prices from different sources
	oracle.prices["BTC"] = newPriceBucket()
	oracle.prices["BTC"].mergeAndUpdateAggregate("source1", &priceUpdate{
		ticker: "BTC",
		price:  50000.0,
		stamp:  now,
		weight: 1.0,
	})
	oracle.prices["BTC"].mergeAndUpdateAggregate("source2", &priceUpdate{
		ticker: "BTC",
		price:  51000.0,
		stamp:  now,
		weight: 1.5,
	})

	contributions := oracle.priceContributions()

	btcRate := contributions["BTC"]
	if btcRate == nil {
		t.Fatal("BTC rate is nil")
	}

	// Verify value is aggregated correctly
	// (50000*1.0 + 51000*1.5) / 2.5 = 50400
	if btcRate.Value == "" {
		t.Error("BTC value is empty")
	}

	// Verify contributions include both sources
	if len(btcRate.Contributions) != 2 {
		t.Errorf("got %d contributions, want 2", len(btcRate.Contributions))
	}

	c1 := btcRate.Contributions["source1"]
	if c1 == nil {
		t.Fatal("source1 contribution is nil")
	}
	if c1.Value == "" {
		t.Error("source1 contribution value is empty")
	}
	if c1.Weight != 1.0 {
		t.Errorf("got weight %v, want 1.0", c1.Weight)
	}
}

// TestFeeRateContributions tests fee rate contribution aggregation.
func TestFeeRateContributions(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)

	now := time.Now()

	// Add fee rates from different sources
	oracle.feeRates["BTC"] = newFeeRateBucket()
	oracle.feeRates["BTC"].mergeAndUpdateAggregate("source1", &feeRateUpdate{
		network: "BTC",
		feeRate: big.NewInt(50),
		stamp:   now,
		weight:  1.0,
	})
	oracle.feeRates["BTC"].mergeAndUpdateAggregate("source2", &feeRateUpdate{
		network: "BTC",
		feeRate: big.NewInt(60),
		stamp:   now,
		weight:  1.0,
	})

	contributions := oracle.feeRateContributions()

	btcRate := contributions["BTC"]
	if btcRate == nil {
		t.Fatal("BTC rate is nil")
	}

	// Verify aggregated value
	if btcRate.Value == "" {
		t.Error("BTC value is empty")
	}

	// Verify contributions
	if len(btcRate.Contributions) != 2 {
		t.Errorf("got %d contributions, want 2", len(btcRate.Contributions))
	}
}

// TestSourcesStatus tests source status assembly.
func TestSourcesStatus(t *testing.T) {
	log := slog.NewBackend(os.Stderr).Logger("test")
	oracle := newTestOracle(log)
	oracle.nodeID = "node1"

	now := time.Now()
	setSourceWeights(oracle, map[string]float64{
		"source1": 1.0,
		"source2": 1.0,
	})

	// Record some fetches
	oracle.fetchTracker.recordFetch("source1", "node1", now)
	oracle.fetchTracker.recordFetch("source1", "node2", now.Add(-time.Hour))
	oracle.fetchTracker.recordFetch("source2", "node1", now.Add(-time.Minute))

	// Add prices for source1
	oracle.prices["BTC"] = newPriceBucket()
	oracle.prices["BTC"].mergeAndUpdateAggregate("source1", &priceUpdate{
		ticker: "BTC",
		price:  50000.0,
		stamp:  now,
		weight: 1.0,
	})

	status := oracle.sourcesStatus()

	if len(status) != 2 {
		t.Errorf("got %d sources, want 2", len(status))
	}

	// Verify source1 status
	s1 := status["source1"]
	if s1 == nil {
		t.Fatal("source1 status is nil")
	}
	if s1.LastFetch == nil {
		t.Error("source1 LastFetch is nil")
	}
	if len(s1.Fetches24h) == 0 {
		t.Error("source1 Fetches24h is empty")
	}
	if len(s1.LatestData) == 0 {
		t.Error("source1 LatestData is empty")
	}

	// Verify source2 status
	s2 := status["source2"]
	if s2 == nil {
		t.Fatal("source2 status is nil")
	}
	if s2.LastFetch == nil {
		t.Error("source2 LastFetch is nil")
	}
}
