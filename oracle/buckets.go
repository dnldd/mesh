package oracle

import (
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

const (
	exponentialDecayHalfLife = time.Minute // 1 minute halflife
	fullValidityPeriod       = exponentialDecayHalfLife
	validityExpiration       = fullValidityPeriod + (time.Minute * 25) // 25 minutes of decay after full validity
)

// agedWeight returns a weight based on the age of an update using exponential decay.
// The weight stays at full strength for fullValidityPeriod, then decays exponentially
// with a half-life of exponentialDecayHalfLife until validityExpiration.
func agedWeight(weight float64, stamp time.Time) float64 {
	age := time.Since(stamp)
	if age < 0 {
		age = 0
	}

	switch {
	case age < fullValidityPeriod:
		return weight
	case age >= validityExpiration:
		return 0
	default:
		elapsedInDecay := age - fullValidityPeriod
		// Use half-life formula: weight * 0.5^(elapsedInDecay / halfLife)
		exponent := float64(elapsedInDecay) / float64(exponentialDecayHalfLife)
		return weight * math.Pow(0.5, exponent)
	}
}

// priceUpdate is the internal message used for when a price update is fetched
// or received from a source.
type priceUpdate struct {
	ticker Ticker
	price  float64
	stamp  time.Time
	weight float64
}

// feeRateUpdate is the internal message used for when a fee rate update is
// fetched or received from a source.
type feeRateUpdate struct {
	network Network
	feeRate *big.Int
	stamp   time.Time
	weight  float64
}

// priceBucket is a collection of price updates from a single source
// and the aggregated price.
type priceBucket struct {
	latest atomic.Uint64

	mtx     sync.RWMutex
	sources map[string]*priceUpdate
}

func newPriceBucket() *priceBucket {
	return &priceBucket{
		latest:  atomic.Uint64{},
		sources: make(map[string]*priceUpdate),
	}
}

func aggregatePriceSources(sources map[string]*priceUpdate) float64 {
	var weightedSum float64
	var totalWeight float64
	for _, entry := range sources {
		weight := agedWeight(entry.weight, entry.stamp)
		if weight == 0 {
			continue
		}
		// Skip zero or negative prices.
		if entry.price <= 0 {
			continue
		}
		totalWeight += weight
		weightedSum += weight * entry.price
	}
	if totalWeight == 0 {
		return 0
	}
	return weightedSum / totalWeight
}

func (b *priceBucket) aggregatedPrice() float64 {
	return math.Float64frombits(b.latest.Load())
}

// mergeAndUpdateAggregate merges a price update into the bucket and returns
// the new updated aggregated price. updated is true if the aggregated price
// was updated, false otherwise (if the update is older than the latest update
// for the source).
func (b *priceBucket) mergeAndUpdateAggregate(source string, upd *priceUpdate) (updated bool, agg float64) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	existing, found := b.sources[source]
	if found && !upd.stamp.After(existing.stamp) {
		return false, 0
	}
	b.sources[source] = upd

	agg = aggregatePriceSources(b.sources)
	b.latest.Store(math.Float64bits(agg))
	return true, agg
}

// feeRateBucket is a collection of fee rate updates from a single source
// and the aggregated fee rate.
type feeRateBucket struct {
	latest atomic.Value // *big.Int

	mtx     sync.RWMutex
	sources map[string]*feeRateUpdate
}

func newFeeRateBucket() *feeRateBucket {
	bucket := &feeRateBucket{
		latest:  atomic.Value{},
		sources: make(map[string]*feeRateUpdate),
	}
	bucket.latest.Store((*big.Int)(nil))
	return bucket
}

func aggregateFeeRateSources(sources map[string]*feeRateUpdate) *big.Int {
	weightedSum := new(big.Float)
	var totalWeight float64

	for _, entry := range sources {
		weight := agedWeight(entry.weight, entry.stamp)
		if weight == 0 {
			continue
		}
		totalWeight += weight

		// Multiply weight (float64) by feeRate (big.Int) using big.Float.
		weightFloat := new(big.Float).SetFloat64(weight)
		feeRateFloat := new(big.Float).SetInt(entry.feeRate)
		product := new(big.Float).Mul(weightFloat, feeRateFloat)
		weightedSum.Add(weightedSum, product)
	}
	if totalWeight == 0 {
		return big.NewInt(0)
	}

	totalWeightFloat := new(big.Float).SetFloat64(totalWeight)
	avgFloat := new(big.Float).Quo(weightedSum, totalWeightFloat)

	// Round to nearest integer.
	if avgFloat.Sign() >= 0 {
		avgFloat.Add(avgFloat, new(big.Float).SetFloat64(0.5))
	} else {
		avgFloat.Sub(avgFloat, new(big.Float).SetFloat64(0.5))
	}
	rounded := new(big.Int)
	avgFloat.Int(rounded)

	return rounded
}

func (b *feeRateBucket) aggregatedRate() *big.Int {
	return b.latest.Load().(*big.Int)
}

// mergeAndUpdateAggregate merges a fee rate update into the bucket and returns
// the new updated aggregated fee rate. updated is true if the aggregated fee rate
// was updated, false otherwise (if the update is older than the latest update
// for the source).
func (b *feeRateBucket) mergeAndUpdateAggregate(source string, upd *feeRateUpdate) (updated bool, agg *big.Int) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	existing, found := b.sources[source]
	if found && !upd.stamp.After(existing.stamp) {
		return false, nil
	}
	b.sources[source] = upd

	agg = aggregateFeeRateSources(b.sources)
	b.latest.Store(agg)
	return true, agg
}

func (o *Oracle) getPriceBucket(ticker Ticker) *priceBucket {
	o.pricesMtx.RLock()
	bucket := o.prices[ticker]
	o.pricesMtx.RUnlock()
	return bucket
}

func (o *Oracle) getOrCreatePriceBucket(ticker Ticker) *priceBucket {
	if bucket := o.getPriceBucket(ticker); bucket != nil {
		return bucket
	}

	o.pricesMtx.Lock()
	defer o.pricesMtx.Unlock()
	if bucket := o.prices[ticker]; bucket != nil {
		return bucket
	}
	bucket := newPriceBucket()
	o.prices[ticker] = bucket
	return bucket
}

func (o *Oracle) getFeeRateBucket(network Network) *feeRateBucket {
	o.feeRatesMtx.RLock()
	bucket := o.feeRates[network]
	o.feeRatesMtx.RUnlock()
	return bucket
}

func (o *Oracle) getOrCreateFeeRateBucket(network Network) *feeRateBucket {
	if bucket := o.getFeeRateBucket(network); bucket != nil {
		return bucket
	}
	o.feeRatesMtx.Lock()
	defer o.feeRatesMtx.Unlock()
	if bucket := o.feeRates[network]; bucket != nil {
		return bucket
	}
	bucket := newFeeRateBucket()
	o.feeRates[network] = bucket
	return bucket
}
