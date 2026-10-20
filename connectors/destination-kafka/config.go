package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-kafka so existing configurations carry over.
type Config struct {
	BootstrapServers string    `json:"bootstrap_servers"`
	TopicPattern     string    `json:"topic_pattern"`
	TestTopic        string    `json:"test_topic"`
	SyncProducer     bool      `json:"sync_producer"`
	Protocol         *Protocol `json:"protocol"`
	ClientID         string    `json:"client_id"`
	Acks             flexText  `json:"acks"`
	EnableIdempotent bool      `json:"enable_idempotence"`
	CompressionType  string    `json:"compression_type"`
	LingerMs         *flexInt  `json:"linger_ms"`
	MaxRequestSize   *flexInt  `json:"max_request_size"`
	MaxInFlight      *flexInt  `json:"max_in_flight_requests_per_connection"`
	Retries          *flexInt  `json:"retries"`
	RequestTimeoutMs *flexInt  `json:"request_timeout_ms"`
	DeliveryTimeout  *flexInt  `json:"delivery_timeout_ms"`
	BufferMemory     *flexInt  `json:"buffer_memory"`
}

// Protocol is how the client talks to the brokers.
type Protocol struct {
	SecurityProtocol string `json:"security_protocol"`
	SASLMechanism    string `json:"sasl_mechanism"`
	SASLUsername     string `json:"sasl_username"`
	SASLPassword     string `json:"sasl_password"`
	SASLJAASConfig   string `json:"sasl_jaas_config"`
}

const (
	protocolPlaintext     = "PLAINTEXT"
	protocolSASLPlaintext = "SASL_PLAINTEXT"
	protocolSASLSSL       = "SASL_SSL"

	mechanismPlain       = "PLAIN"
	mechanismSCRAMSHA256 = "SCRAM-SHA-256"
	mechanismSCRAMSHA512 = "SCRAM-SHA-512"

	acksNone   = "0"
	acksLeader = "1"
	acksAll    = "all"

	defaultClientID        = "synclet-producer"
	defaultAcks            = acksLeader
	defaultCompression     = "none"
	defaultLingerMs        = 0
	defaultMaxRequestSize  = 1048576
	maxMaxRequestSize      = 1 << 30
	defaultMaxInFlight     = 5
	defaultRequestTimeout  = 30000
	defaultDeliveryTimeout = 120000
	defaultBufferMemory    = 33554432

	// unlimitedRetries is the Kafka Java client's default for retries,
	// Integer.MAX_VALUE, which means retrying until the delivery timeout.
	unlimitedRetries = math.MaxInt32
)

var compressions = []string{"none", "gzip", "snappy", "lz4", "zstd"}

// settings is a validated configuration.
type settings struct {
	seeds        []string
	topicPattern string
	testTopic    string
	syncProducer bool

	tls       bool
	mechanism string
	username  string
	password  string

	clientID       string
	acks           string
	idempotent     bool
	compression    string
	linger         time.Duration
	maxRequestSize int32
	maxInFlight    int
	retries        int // unlimitedRetries keeps the client's unlimited default
	requestTimeout time.Duration
	delivery       time.Duration
	bufferMemory   int
}

func (c Config) settings() (settings, error) {
	s := settings{
		topicPattern: c.TopicPattern,
		testTopic:    strings.TrimSpace(c.TestTopic),
		syncProducer: c.SyncProducer,
		clientID:     c.ClientID,
		acks:         string(c.Acks),
		idempotent:   c.EnableIdempotent,
		compression:  c.CompressionType,
	}
	for _, seed := range strings.Split(c.BootstrapServers, ",") {
		if seed = strings.TrimSpace(seed); seed != "" {
			s.seeds = append(s.seeds, seed)
		}
	}
	if len(s.seeds) == 0 {
		return settings{}, errors.New("bootstrap_servers is empty")
	}
	if strings.TrimSpace(s.topicPattern) == "" {
		return settings{}, errors.New("topic_pattern is empty")
	}
	if s.testTopic != "" {
		if err := validTopic(s.testTopic); err != nil {
			return settings{}, fmt.Errorf("test_topic: %w", err)
		}
	}
	if err := c.Protocol.apply(&s); err != nil {
		return settings{}, err
	}
	if s.clientID == "" {
		s.clientID = defaultClientID
	}

	if s.acks == "" {
		s.acks = defaultAcks
	}
	if s.acks == "-1" {
		s.acks = acksAll
	}
	switch s.acks {
	case acksNone, acksLeader, acksAll:
	default:
		return settings{}, fmt.Errorf("acks %q is not one of 0, 1, all", c.Acks)
	}
	if s.idempotent && s.acks != acksAll {
		return settings{}, fmt.Errorf("enable_idempotence needs acks all, not %s", s.acks)
	}

	if s.compression == "" {
		s.compression = defaultCompression
	}
	s.compression = strings.ToLower(s.compression)
	if !slices.Contains(compressions, s.compression) {
		return settings{}, fmt.Errorf("compression_type %q is not one of %s", c.CompressionType, strings.Join(compressions, ", "))
	}

	linger, err := c.LingerMs.orDefault("linger_ms", defaultLingerMs, 0)
	if err != nil {
		return settings{}, err
	}
	s.linger = time.Duration(linger) * time.Millisecond
	maxRequest, err := c.MaxRequestSize.orDefault("max_request_size", defaultMaxRequestSize, 1024)
	if err != nil {
		return settings{}, err
	}
	if maxRequest > maxMaxRequestSize {
		return settings{}, fmt.Errorf("max_request_size %d is larger than %d", maxRequest, maxMaxRequestSize)
	}
	s.maxRequestSize = int32(maxRequest)
	inFlight, err := c.MaxInFlight.orDefault("max_in_flight_requests_per_connection", defaultMaxInFlight, 1)
	if err != nil {
		return settings{}, err
	}
	s.maxInFlight = int(inFlight)
	retries, err := c.Retries.orDefault("retries", unlimitedRetries, 0)
	if err != nil {
		return settings{}, err
	}
	s.retries = int(min(retries, unlimitedRetries))
	reqTimeout, err := c.RequestTimeoutMs.orDefault("request_timeout_ms", defaultRequestTimeout, 1)
	if err != nil {
		return settings{}, err
	}
	s.requestTimeout = time.Duration(reqTimeout) * time.Millisecond
	delivery, err := c.DeliveryTimeout.orDefault("delivery_timeout_ms", defaultDeliveryTimeout, 0)
	if err != nil {
		return settings{}, err
	}
	s.delivery = time.Duration(delivery) * time.Millisecond
	buffer, err := c.BufferMemory.orDefault("buffer_memory", defaultBufferMemory, maxRequest)
	if err != nil {
		return settings{}, err
	}
	s.bufferMemory = int(buffer)
	return s, nil
}

func (p *Protocol) apply(s *settings) error {
	if p == nil || p.SecurityProtocol == "" || p.SecurityProtocol == protocolPlaintext {
		return nil
	}
	switch p.SecurityProtocol {
	case protocolSASLPlaintext:
	case protocolSASLSSL:
		s.tls = true
	default:
		return fmt.Errorf("protocol: security_protocol %q is not one of %s, %s, %s",
			p.SecurityProtocol, protocolPlaintext, protocolSASLPlaintext, protocolSASLSSL)
	}
	s.mechanism = strings.ToUpper(p.SASLMechanism)
	if s.mechanism == "" {
		s.mechanism = mechanismPlain
	}
	switch s.mechanism {
	case mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512:
	default:
		return fmt.Errorf("protocol: sasl_mechanism %q is not one of %s, %s, %s",
			p.SASLMechanism, mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512)
	}
	s.username, s.password = p.SASLUsername, p.SASLPassword
	if s.username == "" && p.SASLJAASConfig != "" {
		user, pass, err := parseJAAS(p.SASLJAASConfig)
		if err != nil {
			return fmt.Errorf("protocol: sasl_jaas_config: %w", err)
		}
		s.username, s.password = user, pass
	}
	if s.username == "" {
		return fmt.Errorf("protocol: %s needs sasl_username and sasl_password, or sasl_jaas_config", p.SecurityProtocol)
	}
	return nil
}

var jaasOption = regexp.MustCompile(`(\w+)\s*=\s*"((?:[^"\\]|\\.)*)"`)

// parseJAAS reads the username and password of a Kafka sasl.jaas.config value
// such as `org.apache.kafka.common.security.plain.PlainLoginModule required
// username="alice" password="secret";`.
func parseJAAS(jaas string) (username, password string, err error) {
	var haveUser, havePass bool
	for _, m := range jaasOption.FindAllStringSubmatch(jaas, -1) {
		value := strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(m[2])
		switch m[1] {
		case "username":
			username, haveUser = value, true
		case "password":
			password, havePass = value, true
		}
	}
	if !haveUser || !havePass {
		return "", "", errors.New(`it has no username="..." and password="..." options`)
	}
	return username, password, nil
}

// flexText is a JSON string or number, kept as its text: Airbyte's acks is a
// string ("1"), but hand-written configurations often say 1.
type flexText string

func (t *flexText) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		*t = ""
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*t = flexText(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("%s is neither a string nor a number", b)
	}
	*t = flexText(n.String())
	return nil
}

// flexInt is a whole number given as a JSON number or a numeric string:
// Airbyte types linger_ms and buffer_memory as strings and the rest as
// integers.
type flexInt struct {
	text string
}

func (f *flexInt) UnmarshalJSON(b []byte) error {
	var t flexText
	if err := t.UnmarshalJSON(b); err != nil {
		return err
	}
	f.text = strings.TrimSpace(string(t))
	return nil
}

func (f *flexInt) orDefault(name string, def, minimum int64) (int64, error) {
	if f == nil || f.text == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(f.text, 10, 64)
	if err != nil {
		v, ferr := strconv.ParseFloat(f.text, 64)
		if ferr != nil || v != math.Trunc(v) || math.Abs(v) > math.MaxInt64/2 {
			return 0, fmt.Errorf("%s %q is not a whole number", name, f.text)
		}
		n = int64(v)
	}
	if n < minimum {
		return 0, fmt.Errorf("%s %d is less than %d", name, n, minimum)
	}
	return n, nil
}
