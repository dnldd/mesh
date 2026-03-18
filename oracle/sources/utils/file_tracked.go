package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

// ResetTimeFunc returns the next reset time for quota.
type ResetTimeFunc func() time.Time

// FileTrackedSourceConfig is the configuration for creating a FileTrackedSource.
type FileTrackedSourceConfig struct {
	Name         string
	Weight       float64
	MinPeriod    time.Duration
	FetchRates   FetchRatesFunc
	FetchesLimit int64
	QuotaFile    string
	ResetTime    ResetTimeFunc
	Log          slog.Logger
}

func (cfg FileTrackedSourceConfig) verify() error {
	var err error

	if cfg.Name == "" {
		err = errors.Join(err, fmt.Errorf("name is required"))
	}
	if cfg.FetchRates == nil {
		err = errors.Join(err, fmt.Errorf("FetchRates is required"))
	}
	if cfg.FetchesLimit <= 0 {
		err = errors.Join(err, fmt.Errorf("FetchesLimit must be positive"))
	}
	if cfg.QuotaFile == "" {
		err = errors.Join(err, fmt.Errorf("QuotaFile is required"))
	}

	if cfg.ResetTime == nil {
		err = errors.Join(err, fmt.Errorf("ResetTime is required"))
	}

	if err != nil {
		return fmt.Errorf("file tracked source config: %w", err)
	}
	return nil
}

// FileTrackedSource implements Source with quota tracking persisted to a local JSON file.
// Fetches reset automatically based on the ResetTime function provided in config.
type FileTrackedSource struct {
	name          string
	weight        float64
	minPeriod     time.Duration
	fetchRates    FetchRatesFunc
	fetchesLimit  int64
	resetTimeFunc ResetTimeFunc

	mtx              sync.Mutex
	fetchesRemaining int64
	resetTime        time.Time
	quotaFile        string
	log              slog.Logger
}

type quotaFileData struct {
	FetchesRemaining int64  `json:"fetches_remaining"`
	ResetTime        string `json:"reset_time"`
}

// NewFileTrackedSource creates a new FileTrackedSource that loads and persists quota to the specified file.
func NewFileTrackedSource(cfg FileTrackedSourceConfig) (*FileTrackedSource, error) {
	if err := cfg.verify(); err != nil {
		return nil, err
	}

	weight := cfg.Weight
	if weight == 0 {
		weight = defaultWeight
	}
	minPeriod := cfg.MinPeriod
	if minPeriod == 0 {
		minPeriod = defaultMinPeriod
	}

	s := &FileTrackedSource{
		name:             cfg.Name,
		weight:           weight,
		minPeriod:        minPeriod,
		fetchRates:       cfg.FetchRates,
		fetchesLimit:     cfg.FetchesLimit,
		fetchesRemaining: cfg.FetchesLimit,
		resetTime:        cfg.ResetTime(),
		resetTimeFunc:    cfg.ResetTime,
		quotaFile:        cfg.QuotaFile,
		log:              cfg.Log,
	}

	s.loadQuotaFromFile()
	return s, nil
}

func (s *FileTrackedSource) loadQuotaFromFile() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	data, err := os.ReadFile(s.quotaFile)
	if err != nil {
		if !os.IsNotExist(err) {
			s.log.Warnf("[%s] Failed to read quota file: %v", s.name, err)
		}
		return
	}

	var qfd quotaFileData
	if err := json.Unmarshal(data, &qfd); err != nil {
		s.log.Warnf("[%s] Failed to parse quota file: %v", s.name, err)
		return
	}

	resetTime, err := time.Parse(time.RFC3339, qfd.ResetTime)
	if err != nil {
		s.log.Warnf("[%s] Failed to parse reset time: %v", s.name, err)
		return
	}

	// If the reset period has passed, refill fetches. Otherwise restore the previous state from file.
	now := time.Now().UTC()
	if now.After(resetTime) {
		s.fetchesRemaining = s.fetchesLimit
		s.resetTime = s.resetTimeFunc()
	} else {
		s.fetchesRemaining = qfd.FetchesRemaining
		s.resetTime = resetTime
	}
}

func (s *FileTrackedSource) saveQuotaToFile(fetches int64, resetTime time.Time) error {
	qfd := quotaFileData{
		FetchesRemaining: fetches,
		ResetTime:        resetTime.Format(time.RFC3339),
	}

	data, err := json.Marshal(qfd)
	if err != nil {
		return fmt.Errorf("[%s] failed to marshal quota: %w", s.name, err)
	}

	if err := AtomicWriteFile(s.quotaFile, data); err != nil {
		return fmt.Errorf("[%s] failed to save quota: %w", s.name, err)
	}
	return nil
}

// Name returns the source name.
func (s *FileTrackedSource) Name() string { return s.name }

// Weight returns the source weight used for result aggregation.
func (s *FileTrackedSource) Weight() float64 { return s.weight }

// MinPeriod returns the minimum time between consecutive fetches.
func (s *FileTrackedSource) MinPeriod() time.Duration { return s.minPeriod }

// FetchRates fetches rates from the source and decrements quota fetches.
// Quota is saved to file before returning.
func (s *FileTrackedSource) FetchRates(ctx context.Context) (*sources.RateInfo, error) {
	rates, err := s.fetchRates(ctx)
	if err != nil {
		return nil, err
	}

	s.mtx.Lock()
	now := time.Now().UTC()
	if now.After(s.resetTime) {
		s.fetchesRemaining = s.fetchesLimit
		s.resetTime = s.resetTimeFunc()
	}

	if s.fetchesRemaining > 0 {
		s.fetchesRemaining--
	}

	fetchesToSave := s.fetchesRemaining
	resetTimeToSave := s.resetTime
	s.mtx.Unlock()

	if err := s.saveQuotaToFile(fetchesToSave, resetTimeToSave); err != nil {
		return nil, err
	}

	return rates, nil
}

// QuotaStatus returns the current quota status, automatically resetting fetches if the period has passed.
func (s *FileTrackedSource) QuotaStatus() *sources.QuotaStatus {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now().UTC()
	if now.After(s.resetTime) {
		s.fetchesRemaining = s.fetchesLimit
		s.resetTime = s.resetTimeFunc()
	}

	return &sources.QuotaStatus{
		FetchesRemaining: s.fetchesRemaining,
		FetchesLimit:     s.fetchesLimit,
		ResetTime:        s.resetTime,
	}
}
