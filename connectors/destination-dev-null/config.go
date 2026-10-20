package main

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-dev-null so existing configurations carry over.
type Config struct {
	TestDestination *TestDestination `json:"test_destination"`
}

// TestDestination selects what the destination does with each record.
type TestDestination struct {
	Type            string         `json:"test_destination_type"`
	LoggingConfig   *LoggingConfig `json:"logging_config"`
	MillisPerRecord *int64         `json:"millis_per_record"`
	NumMessages     *int64         `json:"num_messages"`
}

// LoggingConfig selects which records of each stream the LOGGING mode logs.
type LoggingConfig struct {
	Type          string   `json:"logging_type"`
	MaxEntryCount *float64 `json:"max_entry_count"`
	NthEntryToLog *int64   `json:"nth_entry_to_log"`
	SamplingRatio *float64 `json:"sampling_ratio"`
	Seed          *float64 `json:"seed"`
}

const (
	typeSilent    = "SILENT"
	typeLogging   = "LOGGING"
	typeThrottled = "THROTTLED"
	typeFailing   = "FAILING"

	loggingFirstN         = "FirstN"
	loggingEveryNth       = "EveryNth"
	loggingRandomSampling = "RandomSampling"

	defaultMaxEntryCount = 100
	maxMaxEntryCount     = 1000
	maxNthEntryToLog     = 1000
	defaultSamplingRatio = 0.001
)

// mode is a validated configuration.
type mode struct {
	kind string

	// LOGGING: of each stream, every record whose 0-based index is a multiple
	// of every, kept with probability ratio, is logged until maxEntries.
	maxEntries int64
	every      int64
	ratio      float64
	seed       *uint64

	// THROTTLED
	delay time.Duration

	// FAILING: the record after failAfter records of a stream fails the write.
	failAfter int64
}

func (c Config) mode() (mode, error) {
	td := c.TestDestination
	if td == nil || td.Type == "" {
		return mode{kind: typeSilent}, nil
	}
	switch td.Type {
	case typeSilent:
		return mode{kind: typeSilent}, nil
	case typeLogging:
		if td.LoggingConfig == nil {
			return mode{}, errors.New("test_destination: LOGGING needs logging_config")
		}
		return td.LoggingConfig.mode()
	case typeThrottled:
		if td.MillisPerRecord == nil {
			return mode{}, errors.New("test_destination: THROTTLED needs millis_per_record")
		}
		if *td.MillisPerRecord < 0 {
			return mode{}, fmt.Errorf("test_destination: millis_per_record %d is negative", *td.MillisPerRecord)
		}
		return mode{kind: typeThrottled, delay: time.Duration(*td.MillisPerRecord) * time.Millisecond}, nil
	case typeFailing:
		if td.NumMessages == nil {
			return mode{}, errors.New("test_destination: FAILING needs num_messages")
		}
		if *td.NumMessages < 0 {
			return mode{}, fmt.Errorf("test_destination: num_messages %d is negative", *td.NumMessages)
		}
		return mode{kind: typeFailing, failAfter: *td.NumMessages}, nil
	default:
		return mode{}, fmt.Errorf("test_destination: test_destination_type %q is not one of %s, %s, %s, %s",
			td.Type, typeSilent, typeLogging, typeThrottled, typeFailing)
	}
}

func (l LoggingConfig) mode() (mode, error) {
	m := mode{kind: typeLogging, maxEntries: defaultMaxEntryCount, every: 1, ratio: 1}
	if l.MaxEntryCount != nil {
		n := *l.MaxEntryCount
		if n != math.Trunc(n) || n < 1 || n > maxMaxEntryCount {
			return mode{}, fmt.Errorf("logging_config: max_entry_count %v is not a whole number from 1 to %d", n, maxMaxEntryCount)
		}
		m.maxEntries = int64(n)
	}
	switch l.Type {
	case loggingFirstN:
	case loggingEveryNth:
		if l.NthEntryToLog == nil {
			return mode{}, errors.New("logging_config: EveryNth needs nth_entry_to_log")
		}
		if n := *l.NthEntryToLog; n < 1 || n > maxNthEntryToLog {
			return mode{}, fmt.Errorf("logging_config: nth_entry_to_log %d is not from 1 to %d", n, maxNthEntryToLog)
		}
		m.every = *l.NthEntryToLog
	case loggingRandomSampling:
		m.ratio = defaultSamplingRatio
		if l.SamplingRatio != nil {
			m.ratio = *l.SamplingRatio
		}
		if m.ratio < 0 || m.ratio > 1 {
			return mode{}, fmt.Errorf("logging_config: sampling_ratio %v is not from 0 to 1", m.ratio)
		}
		if l.Seed != nil {
			s := uint64(int64(*l.Seed))
			m.seed = &s
		}
	default:
		return mode{}, fmt.Errorf("logging_config: logging_type %q is not one of %s, %s, %s",
			l.Type, loggingFirstN, loggingEveryNth, loggingRandomSampling)
	}
	return m, nil
}
