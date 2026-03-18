package oracle

import (
	"sync"
	"time"

	"github.com/decred/slog"
)

const trackingPeriod = 24 * time.Hour

// fetchRecord represents a single fetch event.
type fetchRecord struct {
	SourceID uint16
	NodeID   uint16
	Stamp    time.Time
}

// fetchTracker tracks fetch events for the past 24 hours.
type fetchTracker struct {
	log     slog.Logger
	mtx     sync.Mutex
	records []fetchRecord
	// To reduce memory, records store uint16 IDs rather than full strings.
	// ID mappings are append-only and bounded by uint16 max (65535).
	sourceIDs    map[string]uint16
	nodeIDs      map[string]uint16
	sourceNames  []string
	nodeNames    []string
	nextSourceID uint16
	nextNodeID   uint16
	counts       map[uint16]map[uint16]int
	latest       map[uint16]time.Time
}

// newFetchTracker creates a new fetchTracker.
func newFetchTracker(log slog.Logger) *fetchTracker {
	return &fetchTracker{
		log:       log,
		sourceIDs: make(map[string]uint16),
		nodeIDs:   make(map[string]uint16),
		counts:    make(map[uint16]map[uint16]int),
		latest:    make(map[uint16]time.Time),
	}
}

// recordFetch records a fetch event.
func (ft *fetchTracker) recordFetch(source, nodeID string, stamp time.Time) {
	ft.mtx.Lock()
	defer ft.mtx.Unlock()
	sourceID, ok := assignID(source, ft.sourceIDs, &ft.sourceNames, &ft.nextSourceID)
	if !ok {
		ft.log.Warnf("fetchTracker: source ID space exhausted (max 65535), fetch tracking disabled for source: %s", source)
		return
	}
	nodeIDInt, ok := assignID(nodeID, ft.nodeIDs, &ft.nodeNames, &ft.nextNodeID)
	if !ok {
		ft.log.Warnf("fetchTracker: node ID space exhausted (max 65535), fetch tracking disabled for node: %s", nodeID)
		return
	}
	r := fetchRecord{
		SourceID: sourceID,
		NodeID:   nodeIDInt,
		Stamp:    stamp,
	}

	// Insert in sorted order by stamp. Almost always appends since records
	// arrive roughly chronologically.
	i := len(ft.records)
	for i > 0 && ft.records[i-1].Stamp.After(stamp) {
		i--
	}
	ft.records = append(ft.records, r)
	if i < len(ft.records)-1 {
		copy(ft.records[i+1:], ft.records[i:len(ft.records)-1])
		ft.records[i] = r
	}

	// Update the counts and latest fetch for the source.
	if ft.counts[sourceID] == nil {
		ft.counts[sourceID] = make(map[uint16]int)
	}
	ft.counts[sourceID][nodeIDInt]++
	if existing, ok := ft.latest[sourceID]; !ok || stamp.After(existing) {
		ft.latest[sourceID] = stamp
	}
}

// assignID returns the uint16 ID for name, creating one if needed. Returns
// false if the ID space (uint16) is exhausted.
func assignID(name string, ids map[string]uint16, names *[]string, next *uint16) (uint16, bool) {
	if id, ok := ids[name]; ok {
		return id, true
	}
	if *next == ^uint16(0) {
		return 0, false
	}
	id := *next
	*next++
	ids[name] = id
	*names = append(*names, name)
	return id, true
}

// dropExpired removes records older than the given cutoff. Records are kept
// sorted by stamp, so we scan from the front and stop at the first non-expired.
// ft.mtx MUST be locked when calling this function.
func (ft *fetchTracker) dropExpired(cutoff time.Time) {
	expiredCount := 0
	for expiredCount < len(ft.records) && ft.records[expiredCount].Stamp.Before(cutoff) {
		r := ft.records[expiredCount]
		if nodes, ok := ft.counts[r.SourceID]; ok {
			if nodes[r.NodeID] > 1 {
				nodes[r.NodeID]--
			} else {
				delete(nodes, r.NodeID)
				if len(nodes) == 0 {
					delete(ft.counts, r.SourceID)
				}
			}
		}
		expiredCount++
	}
	if expiredCount > 0 {
		ft.records = ft.records[expiredCount:]
	}
	for sourceID, stamp := range ft.latest {
		if stamp.Before(cutoff) {
			delete(ft.latest, sourceID)
		}
	}
}

// sourceFetchCounts returns per-node fetch counts for a single source over the
// past 24 hours.
func (ft *fetchTracker) sourceFetchCounts(source string) map[string]int {
	ft.mtx.Lock()
	defer ft.mtx.Unlock()
	ft.dropExpired(time.Now().Add(-trackingPeriod))
	sourceID, ok := ft.sourceIDs[source]
	if !ok {
		return nil
	}
	nodes, ok := ft.counts[sourceID]
	if !ok {
		return nil
	}
	result := make(map[string]int, len(nodes))
	for nodeID, count := range nodes {
		if name, ok := ft.nodeName(nodeID); ok {
			result[name] = count
		}
	}
	return result
}

// fetchCounts returns per-source, per-node fetch counts for the past 24 hours.
func (ft *fetchTracker) fetchCounts() map[string]map[string]int {
	ft.mtx.Lock()
	defer ft.mtx.Unlock()
	ft.dropExpired(time.Now().Add(-trackingPeriod))
	result := make(map[string]map[string]int, len(ft.counts))
	for sourceID, nodes := range ft.counts {
		source, ok := ft.sourceName(sourceID)
		if !ok {
			continue
		}
		nodeMap := make(map[string]int, len(nodes))
		for nodeID, count := range nodes {
			if name, ok := ft.nodeName(nodeID); ok {
				nodeMap[name] = count
			}
		}
		result[source] = nodeMap
	}
	return result
}

// latestPerSource returns the most recent fetch timestamp per source.
func (ft *fetchTracker) latestPerSource() map[string]time.Time {
	ft.mtx.Lock()
	defer ft.mtx.Unlock()
	ft.dropExpired(time.Now().Add(-trackingPeriod))
	result := make(map[string]time.Time, len(ft.latest))
	for sourceID, stamp := range ft.latest {
		if source, ok := ft.sourceName(sourceID); ok {
			result[source] = stamp
		}
	}
	return result
}

func (ft *fetchTracker) sourceName(id uint16) (string, bool) {
	if int(id) >= len(ft.sourceNames) {
		return "", false
	}
	return ft.sourceNames[id], true
}

func (ft *fetchTracker) nodeName(id uint16) (string, bool) {
	if int(id) >= len(ft.nodeNames) {
		return "", false
	}
	return ft.nodeNames[id], true
}
