# Oracle Package Audit Report
**Date**: 2026-03-16
**Overall Coverage**: 73.5% (sources/utils), 59.4% (oracle.New), varies widely across providers

---

## 🔴 CRITICAL Issues

### 1. Goroutine Leak in diviner.fetchUpdates
**File**: `oracle/diviner.go:99-103`
**Severity**: CRITICAL - Memory leak under high publish failure rates

```go
go func() {
    if err := d.publishUpdate(ctx, update); err != nil {
        d.log.Errorf("Failed to publish oracle update: %v", err)
    }
}()
```

**Problem**:
- Spawns goroutine without tracking or wait group
- If `publishUpdate` blocks (network issue, full channel), goroutines accumulate
- With many sources and publish failures, can exhaust goroutine budget

**Impact**: Memory exhaustion, process degradation under network failures

**Fix**: Use bounded goroutine pool or add timeout to `publishUpdate`

---

### 2. Async Quota Persistence Race Condition
**File**: `oracle/sources/utils/file_tracked.go:195-199`

```go
go func() {
    if err := s.saveQuotaToFile(fetchesToSave, resetTimeToSave); err != nil {
        s.log.Warnf(err.Error())
    }
}()
```

**Problem**:
- Quota saved asynchronously after FetchRates returns
- Process crash → quota loss → overdraft of API calls
- Violates integrity: quota decrement is promised but not guaranteed

**Impact**: API quota violations, overages, service failures

**Fix**: Save quota synchronously before returning from FetchRates, or wrap in atomic operation

---

### 3. Unsafe Context Usage in Goroutines
**File**: `oracle/diviner.go:115-151` (diviner.run method)

**Problem**:
- `ctx` captured in closure at line 100 (used in goroutine)
- Goroutine inherits Run's context, but may outlive it
- If context cancelled during fetch, subsequent retries still use stale context

**Impact**: Context leaks, improper cancellation handling

**Fix**: Create child contexts in goroutines, or pass context explicitly

---

## 🟠 HIGH Issues

### 1. Missing OracleSnapshot Coverage
**File**: `oracle/snapshot.go:51-244`
**Coverage**: 0% for sourcesStatus(), priceContributions(), feeRateContributions(), OracleSnapshot()

**Problem**:
- Critical public API functions untested
- No validation that snapshot structure is correct
- Concurrent access patterns not exercised
- Clients depending on OracleSnapshot contract have no guarantee of correctness

**Tests Needed**:
- [ ] Test OracleSnapshot() with populated oracle state
- [ ] Test concurrent Merge() and OracleSnapshot() calls (race detector)
- [ ] Test snapshot with nil bucket values
- [ ] Test with empty oracle state

---

### 2. Unimplemented Provider Coverage
**File**: `oracle/sources/providers/`
**Coverage**: 0% for bitcore (3 functions), firo (1 function), coingecko pro tier (1 function)

**Providers**:
- `bitcore.go`: NewBitcoreBitcoinCashSource, NewBitcoreDogecoinSource, NewBitcoreLitecoinSource
- `firo.go`: NewFiroOrgSource
- `coingecko.go`: newCoinGeckoProSource, coingeckoProQuotaFetcher

**Problem**:
- No tests mean live API integration never verified
- Parser functions exist (66-90% coverage) but initialization paths untested
- API changes would go undetected

---

### 3. HTTP Helper Functions Untested
**File**: `oracle/sources/utils/http.go:37-96`
**Coverage**: DoGet() 0%, StreamDecodeJSON() 0%

**Problem**:
- Core HTTP logic never exercised
- Error handling for non-2xx responses not validated
- JSON decoding edge cases (trailing data, oversized responses) not tested
- HTTP header construction not verified

**Tests Needed**:
- [ ] Test DoGet with 200, 404, 500 responses
- [ ] Test header merging with multiple headers
- [ ] Test StreamDecodeJSON with trailing JSON, oversized payloads
- [ ] Test context cancellation in DoGet

---

### 4. Network Schedule Query Missing Coverage
**File**: `oracle/quota_manager.go:213`
**Coverage**: 0% for getNetworkSchedule()

**Problem**:
- Called from diviner.fetchScheduleInfo() but never tested
- Returns critical scheduling info used for fetch timing
- Untested lock acquisition patterns

---

### 5. Tatum Provider Parser Coverage Gaps
**File**: `oracle/sources/providers/tatum.go`
**Coverage**: tatumQuotaFetcher (66.7%), tatumParserForNetwork (71.4%)

**Problem**:
- Error paths in quota fetching not tested
- Network-specific parser routing not exercised

---

## 🟡 MEDIUM Issues

### 1. Low Coverage in Critical Paths

**diviner.go**:
- fetchScheduleInfo() - 66.7%: Missing error info handling
- fetchErrorInfo() - 85.7%: Type assertion not fully tested
- run() - 84.6%: Some timer branches untested

**quota_manager.go**:
- run() - 62.5%: Main loop branches not fully exercised
- getActivePeersForSource() - 61.5%: Peer filtering logic gaps

**oracle.go**:
- New() - 59.4%: CoinGecko config path not fully tested
- Price() - 83.3%: Edge cases with nil bucket

**Actions**:
- Add tests for error branches (fetchScheduleInfo nil check)
- Test quota manager's periodic heartbeat and cleanup paths
- Test oracle.New() with all API key combinations

---

### 2. Potential Deadlock in snapshot.sourcesStatus()
**File**: `oracle/snapshot.go:51-164`

**Problem**:
- Acquires locks in order: divinersMtx → pricesMtx → feeRatesMtx
- Other code paths may acquire in different order
- While locks are held, calls external functions (fireScheduleChanged)

**Example Risk**:
- sourcesStatus() holds divinersMtx
- Meanwhile, Merge() acquires divinersMtx via rescheduleDiviner()
- If either triggers callback during lock hold → potential deadlock

**Fix**: Review all lock acquisition orders, avoid callbacks while holding locks

---

### 3. File Tracking Edge Cases
**File**: `oracle/sources/utils/file_tracked.go`

**Gaps**:
- AtomicWriteFile() - 57.1%: Error handling in atomic write not fully tested
- FetchesRemaining floors at zero - tested (100%), but race condition if concurrent calls not synchronized

---

## 🔵 LOW Issues

### 1. Blockcypher Quota Fetcher
**File**: `oracle/sources/providers/blockcypher.go:54`
**Coverage**: 6.2% - blockcypherQuotaFetcher

**Problem**: HTTP error handling path not exercised

---

### 2. Unused Error Info Branches
**File**: `oracle/diviner.go:168`

**In fireScheduleChanged()**:
```go
if errMsg != "" && errStamp != nil {  // Line 168
    status.LastError = errMsg
    status.LastErrorTime = errStamp
}
```

Condition `errStamp != nil` is always true when errMsg != "" (fetchErrorInfo guarantees it)

---

## 📊 Coverage Summary by Package

| Package | Coverage | Status |
|---------|----------|--------|
| oracle (main) | 83.3% avg | Good, snapshot functions need tests |
| diviner | 84.8% avg | Good, error paths untested |
| quota_manager | 78.4% avg | Fair, run() and peer filtering gaps |
| fetch_tracker | 88.2% avg | Good, minor edge cases |
| snapshot | 0% public API | **CRITICAL** |
| sources/providers | Varies 0-92% | **CRITICAL** - bitcore, firo, coingecko pro untested |
| sources/utils | 73.5% | Fair, http functions untested |

---

## 🎯 Recommended Actions (Priority Order)

### Immediate (Before Production)
1. **Fix async quota persistence** (file_tracked.go) - Critical data integrity
2. **Test OracleSnapshot()** - Critical public API
3. **Add DoGet/StreamDecodeJSON tests** - Core HTTP logic
4. **Review lock order for deadlock risk** - Concurrency safety

### Short Term
5. Add coingecko pro tier tests
6. Test bitcore and firo providers
7. Cover quota_manager.run() branches
8. Add tests for diviner error paths

### Medium Term
9. Audit goroutine lifetime in diviner
10. Add concurrent access tests (race detector)
11. Improve file_tracked edge case coverage
12. Test tatum parser network-specific paths

---

## Test Results
- **All tests pass**: ✅ Yes (with race detector)
- **Total test suites**: 10
- **Flakey tests**: None detected
- **Race conditions detected**: None (but async patterns warrant review)

---

## Related Skills
- **gocraft**: Use to implement missing tests and fix async patterns
- **code-review**: Review quota persistence refactor and concurrency fixes
- **simplify**: Clean up after test additions

