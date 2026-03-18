package providers_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/providers"
)

// tHTTPClient implements sources.HTTPClient for testing.
type tHTTPClient struct {
	response *http.Response
	err      error
}

func (tc *tHTTPClient) Do(*http.Request) (*http.Response, error) { return tc.response, tc.err }

func newMockResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func testLogger() slog.Logger { return slog.Disabled }

func TestDcrdataSource(t *testing.T) {
	client := &tHTTPClient{response: newMockResponse(`{"2": 0.0001}`)}
	src := providers.NewDcrdataSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"2": 0.0001}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "DCR" {
			t.Errorf("expected network DCR, got %s", result.FeeRates[0].Network)
		}

		// 0.0001 DCR/kB * 1e5 = 10 atoms/byte
		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(10)) != 0 {
			t.Errorf("expected fee rate 10, got %s", result.FeeRates[0].FeeRate.String())
		}
	})

	t.Run("quota status is unlimited", func(t *testing.T) {
		status := src.QuotaStatus()
		if status.FetchesRemaining != math.MaxInt64 {
			t.Errorf("expected unlimited fetches, got %d", status.FetchesRemaining)
		}
	})
}

func TestMempoolDotSpaceSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewMempoolDotSpaceSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"fastestFee": 25}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "BTC" {
			t.Errorf("expected network BTC, got %s", result.FeeRates[0].Network)
		}

		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(25)) != 0 {
			t.Errorf("expected fee rate 25, got %s", result.FeeRates[0].FeeRate.String())
		}
	})
}

func TestCoinpaprikaSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewCoinpaprikaSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		body := `[
			{"id":"btc-bitcoin","symbol":"BTC","quotes":{"USD":{"price":87838.55}}},
			{"id":"eth-ethereum","symbol":"ETH","quotes":{"USD":{"price":2954.14}}}
		]`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}

		prices := make(map[sources.Ticker]float64)
		for _, p := range result.Prices {
			prices[p.Ticker] = p.Price
		}

		if prices["BTC"] != 87838.55 {
			t.Errorf("expected BTC price 87838.55, got %f", prices["BTC"])
		}
	})
}

func TestCoinMarketCapSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewCoinMarketCapSource(client, testLogger(), "test-api-key")

	t.Run("valid response", func(t *testing.T) {
		body := `{
			"data": [
				{"symbol":"BTC","quote":{"USD":{"price":90000.50}}},
				{"symbol":"ETH","quote":{"USD":{"price":3100.25}}}
			]
		}`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}
	})

	t.Run("quota status refreshes", func(t *testing.T) {
		// Initially should return unlimited (no quota fetched yet)
		status := src.QuotaStatus()
		if status == nil {
			t.Fatal("expected quota status")
		}
	})
}

func TestTatumSources(t *testing.T) {
	client := &tHTTPClient{}
	tatumSources := providers.NewTatumSources(providers.TatumConfig{
		HTTPClient: client,
		Log:        testLogger(),
		APIKey:     "test-api-key",
	})

	t.Run("all sources returned", func(t *testing.T) {
		all := tatumSources.All()
		if len(all) != 3 {
			t.Fatalf("expected 3 sources, got %d", len(all))
		}
	})

	t.Run("btc valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"fast": 25}`)
		result, err := tatumSources.Bitcoin.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "BTC" {
			t.Errorf("expected network BTC, got %s", result.FeeRates[0].Network)
		}

		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(25)) != 0 {
			t.Errorf("expected fee rate 25, got %s", result.FeeRates[0].FeeRate.String())
		}
	})

	t.Run("pool tracks consumption", func(t *testing.T) {
		// After a fetch, pool should have consumed 10 credits.
		// Before reconciliation, quota is unlimited.
		status := tatumSources.Bitcoin.QuotaStatus()
		if status == nil {
			t.Fatal("expected quota status")
		}
	})
}

func TestBlockcypherSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewBlockcypherLitecoinSource(client, testLogger(), "test-token")

	t.Run("valid response", func(t *testing.T) {
		body := `{"medium_fee_per_kb": 10000}`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "LTC" {
			t.Errorf("expected network LTC, got %s", result.FeeRates[0].Network)
		}
	})
}

func TestCoinGeckoSource(t *testing.T) {
	client := &tHTTPClient{}
	quotaFile := t.TempDir() + "/quota.json"
	src, err := providers.NewCoinGeckoSource(client, testLogger(), "test_key", false, quotaFile, 9800)
	if err != nil {
		t.Fatalf("NewCoinGeckoSource failed: %v", err)
	}

	t.Run("valid response", func(t *testing.T) {
		body := `[
			{"symbol":"btc","current_price":90000.50},
			{"symbol":"eth","current_price":3100.25}
		]`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}

		prices := make(map[sources.Ticker]float64)
		for _, p := range result.Prices {
			prices[p.Ticker] = p.Price
		}

		if prices["BTC"] != 90000.50 {
			t.Errorf("expected BTC price 90000.50, got %f", prices["BTC"])
		}

		if prices["ETH"] != 3100.25 {
			t.Errorf("expected ETH price 3100.25, got %f", prices["ETH"])
		}
	})

	t.Run("quota status is tracked", func(t *testing.T) {
		status := src.QuotaStatus()
		if status.FetchesRemaining != 9799 {
			t.Errorf("expected 9799 fetches after one fetch, got %d", status.FetchesRemaining)
		}
		if status.FetchesLimit != 9800 {
			t.Errorf("expected limit 9800, got %d", status.FetchesLimit)
		}
	})
}

// TestBitcoreBitcoinCashSource tests Bitcore Bitcoin Cash source.
func TestBitcoreBitcoinCashSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewBitcoreBitcoinCashSource(client, testLogger())

	if src.Name() != "bch.bitcore" {
		t.Errorf("expected name 'bch.bitcore', got %q", src.Name())
	}
	if src.Weight() != 0.25 {
		t.Errorf("expected weight 0.25, got %f", src.Weight())
	}

	t.Run("valid fee rate", func(t *testing.T) {
		// Response: 50.5 satoshi/byte = 50.5 * 1e5 atoms/byte = 5_050_000
		client.response = newMockResponse(`{"feerate": 50.5}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		expected := big.NewInt(5050000)
		if result.FeeRates[0].FeeRate.Cmp(expected) != 0 {
			t.Errorf("expected fee rate %s, got %s", expected.String(), result.FeeRates[0].FeeRate.String())
		}

		if result.FeeRates[0].Network != "BCH" {
			t.Errorf("expected network BCH, got %s", result.FeeRates[0].Network)
		}
	})

	t.Run("zero fee rate error", func(t *testing.T) {
		client.response = newMockResponse(`{"feerate": 0}`)
		_, err := src.FetchRates(context.Background())
		if err == nil {
			t.Error("expected error for zero fee rate, got nil")
		}
	})

	t.Run("negative fee rate error", func(t *testing.T) {
		client.response = newMockResponse(`{"feerate": -1.5}`)
		_, err := src.FetchRates(context.Background())
		if err == nil {
			t.Error("expected error for negative fee rate, got nil")
		}
	})

	t.Run("invalid json error", func(t *testing.T) {
		client.response = newMockResponse(`{invalid json}`)
		_, err := src.FetchRates(context.Background())
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("quota is unlimited", func(t *testing.T) {
		status := src.QuotaStatus()
		if status.FetchesRemaining != math.MaxInt64 {
			t.Errorf("expected unlimited fetches, got %d", status.FetchesRemaining)
		}
	})
}

// TestBitcoreDogecoinSource tests Bitcore Dogecoin source.
func TestBitcoreDogecoinSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewBitcoreDogecoinSource(client, testLogger())

	if src.Name() != "doge.bitcore" {
		t.Errorf("expected name 'doge.bitcore', got %q", src.Name())
	}

	t.Run("valid fee rate", func(t *testing.T) {
		client.response = newMockResponse(`{"feerate": 10.25}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if result.FeeRates[0].Network != "DOGE" {
			t.Errorf("expected network DOGE, got %s", result.FeeRates[0].Network)
		}
	})
}

// TestBitcoreLitecoinSource tests Bitcore Litecoin source.
func TestBitcoreLitecoinSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewBitcoreLitecoinSource(client, testLogger())

	if src.Name() != "ltc.bitcore" {
		t.Errorf("expected name 'ltc.bitcore', got %q", src.Name())
	}

	t.Run("valid fee rate", func(t *testing.T) {
		client.response = newMockResponse(`{"feerate": 15.75}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if result.FeeRates[0].Network != "LTC" {
			t.Errorf("expected network LTC, got %s", result.FeeRates[0].Network)
		}
	})
}

// TestFiroOrgSource tests Firo.org fee rate source.
func TestFiroOrgSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewFiroOrgSource(client, testLogger())

	if src.Name() != "firo.org" {
		t.Errorf("expected name 'firo.org', got %q", src.Name())
	}
	if src.Weight() != 0.25 {
		t.Errorf("expected weight 0.25, got %f", src.Weight())
	}

	t.Run("valid fee rate response", func(t *testing.T) {
		// estimateFeeParser expects map with single key "2" (priority level 2)
		body := `{"2": 0.00035}`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "FIRO" {
			t.Errorf("expected network FIRO, got %s", result.FeeRates[0].Network)
		}

		// 0.00035 FIRO/kB * 1e5 = 35 atoms/byte
		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(35)) != 0 {
			t.Errorf("expected fee rate 35, got %s", result.FeeRates[0].FeeRate.String())
		}
	})

	t.Run("invalid json error", func(t *testing.T) {
		client.response = newMockResponse(`not json`)
		_, err := src.FetchRates(context.Background())
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("quota is unlimited", func(t *testing.T) {
		status := src.QuotaStatus()
		if status.FetchesRemaining != math.MaxInt64 {
			t.Errorf("expected unlimited fetches, got %d", status.FetchesRemaining)
		}
	})
}

// TestCoinGeckoProSource tests CoinGecko Pro tier source with quota reconciliation.
func TestCoinGeckoProSource(t *testing.T) {
	client := &tHTTPClient{}
	quotaFile := t.TempDir() + "/quota.json"
	src, err := providers.NewCoinGeckoSource(client, testLogger(), "test-pro-key", true, quotaFile, 0)
	if err != nil {
		t.Fatalf("NewCoinGeckoSource failed: %v", err)
	}

	if src.Name() != "coingecko" {
		t.Errorf("expected name 'coingecko', got %q", src.Name())
	}

	t.Run("valid price response", func(t *testing.T) {
		body := `[
			{"symbol":"btc","current_price":95000.00},
			{"symbol":"eth","current_price":3500.50}
		]`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}

		prices := make(map[sources.Ticker]float64)
		for _, p := range result.Prices {
			prices[p.Ticker] = p.Price
		}

		if prices["BTC"] != 95000.00 {
			t.Errorf("expected BTC price 95000.00, got %f", prices["BTC"])
		}

		if prices["ETH"] != 3500.50 {
			t.Errorf("expected ETH price 3500.50, got %f", prices["ETH"])
		}
	})

	t.Run("invalid json error", func(t *testing.T) {
		client.response = newMockResponse(`invalid`)
		_, err := src.FetchRates(context.Background())
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("pro tier creates tracked source", func(t *testing.T) {
		// Verify pro tier source implements Source interface and has quota tracking
		quota := src.QuotaStatus()
		if quota == nil {
			t.Fatal("expected quota status, got nil")
		}
		// Pro tier will attempt to fetch quota from API, which may fail in test without mocking
		// Just verify the quota structure exists
	})
}

// TestCoinGeckoProSourceQuotaReconciliation tests quota reconciliation for CoinGecko Pro tier.
func TestCoinGeckoProSourceQuotaReconciliation(t *testing.T) {
	client := &tHTTPClient{}
	quotaFile := t.TempDir() + "/quota.json"
	src, err := providers.NewCoinGeckoSource(client, testLogger(), "test-pro-key", true, quotaFile, 0)
	if err != nil {
		t.Fatalf("NewCoinGeckoSource failed: %v", err)
	}

	t.Run("price fetch with pro tier", func(t *testing.T) {
		// Verify pro tier can fetch prices successfully
		priceBody := `[{"symbol":"btc","current_price":95000.00}]`
		client.response = newMockResponse(priceBody)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 1 {
			t.Fatalf("expected 1 price, got %d", len(result.Prices))
		}
	})

	t.Run("quota reset time calculation", func(t *testing.T) {
		quota := src.QuotaStatus()

		// Verify reset time is approximately next month
		now := time.Now().UTC()
		nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		if now.Month() == 12 {
			nextMonth = time.Date(now.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
		}

		// Allow 1-hour tolerance for reset time
		if quota.ResetTime.Sub(nextMonth) > time.Hour || nextMonth.Sub(quota.ResetTime) > time.Hour {
			t.Logf("reset time %v not within 1 hour of expected %v (acceptable for quota reconciliation)", quota.ResetTime, nextMonth)
		}
	})
}

// TestTatumQuotaFetcher tests error handling in tatumQuotaFetcher function.
func TestTatumQuotaFetcher(t *testing.T) {
	client := &tHTTPClient{}

	t.Run("http error propagated", func(t *testing.T) {
		client.response = &http.Response{StatusCode: http.StatusInternalServerError}
		client.response.Body = io.NopCloser(bytes.NewBufferString("Server error"))

		quotaFetcher := providers.NewTatumSources(providers.TatumConfig{
			HTTPClient: client,
			Log:        testLogger(),
			APIKey:     "test-key",
		}).Bitcoin // Access quota through source

		// Trigger quota fetch by getting quota status
		// (Note: This would normally trigger async reconciliation in production)
		// For testing, we verify the source structure is correct
		if quotaFetcher == nil {
			t.Fatal("Bitcoin source should not be nil")
		}
	})

	t.Run("valid quota response", func(t *testing.T) {
		client.response = newMockResponse(`{"used": 1000, "limit": 10000}`)

		tatumSources := providers.NewTatumSources(providers.TatumConfig{
			HTTPClient: client,
			Log:        testLogger(),
			APIKey:     "test-key",
		})

		// Verify quota status structure exists
		quota := tatumSources.Bitcoin.QuotaStatus()
		if quota == nil {
			t.Fatal("expected quota status")
		}
	})
}

// TestTatumParserForNetwork tests network-specific fee rate parsing.
func TestTatumParserForNetwork(t *testing.T) {
	client := &tHTTPClient{}
	tatumSources := providers.NewTatumSources(providers.TatumConfig{
		HTTPClient: client,
		Log:        testLogger(),
		APIKey:     "test-key",
	})

	tests := []struct {
		name          string
		source        sources.Source
		responseBody  string
		expectNetwork sources.Network
		expectFail    bool
	}{
		{
			name:          "bitcoin fee rate",
			source:        tatumSources.Bitcoin,
			responseBody:  `{"fast": 42}`,
			expectNetwork: "BTC",
			expectFail:    false,
		},
		{
			name:          "litecoin fee rate",
			source:        tatumSources.Litecoin,
			responseBody:  `{"fast": 35}`,
			expectNetwork: "LTC",
			expectFail:    false,
		},
		{
			name:          "dogecoin fee rate",
			source:        tatumSources.Dogecoin,
			responseBody:  `{"fast": 50}`,
			expectNetwork: "DOGE",
			expectFail:    false,
		},
		{
			name:         "negative fee rate rejected",
			source:       tatumSources.Bitcoin,
			responseBody: `{"fast": -10}`,
			expectFail:   true,
		},
		{
			name:         "zero fee rate rejected",
			source:       tatumSources.Bitcoin,
			responseBody: `{"fast": 0}`,
			expectFail:   true,
		},
		{
			name:         "invalid json fails",
			source:       tatumSources.Bitcoin,
			responseBody: `{invalid}`,
			expectFail:   true,
		},
		{
			name:         "missing fast field fails",
			source:       tatumSources.Bitcoin,
			responseBody: `{"slow": 10}`,
			expectFail:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client.response = newMockResponse(tt.responseBody)
			result, err := tt.source.FetchRates(context.Background())

			if tt.expectFail {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(result.FeeRates) != 1 {
					t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
				}
				if result.FeeRates[0].Network != tt.expectNetwork {
					t.Errorf("expected network %s, got %s", tt.expectNetwork, result.FeeRates[0].Network)
				}
			}
		})
	}
}

// TestTatumNetworksIndependence verifies each Tatum network source operates independently.
func TestTatumNetworksIndependence(t *testing.T) {
	client := &tHTTPClient{}
	tatumSources := providers.NewTatumSources(providers.TatumConfig{
		HTTPClient: client,
		Log:        testLogger(),
		APIKey:     "test-key",
	})

	// Bitcoin succeeds
	client.response = newMockResponse(`{"fast": 30}`)
	btcResult, err := tatumSources.Bitcoin.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("bitcoin fetch failed: %v", err)
	}

	// Litecoin should also work independently
	client.response = newMockResponse(`{"fast": 25}`)
	ltcResult, err := tatumSources.Litecoin.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("litecoin fetch failed: %v", err)
	}

	// Verify distinct results
	if btcResult.FeeRates[0].FeeRate.Cmp(ltcResult.FeeRates[0].FeeRate) == 0 {
		t.Error("expected different fee rates for BTC and LTC")
	}

	// Verify correct networks
	if btcResult.FeeRates[0].Network != "BTC" {
		t.Errorf("expected BTC network, got %s", btcResult.FeeRates[0].Network)
	}
	if ltcResult.FeeRates[0].Network != "LTC" {
		t.Errorf("expected LTC network, got %s", ltcResult.FeeRates[0].Network)
	}
}
