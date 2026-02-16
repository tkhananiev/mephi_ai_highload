package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// =====================
// Models
// =====================

type MetricIngest struct {
	Timestamp time.Time `json:"timestamp"`
	CPU       float64   `json:"cpu"` // 0..100
	RPS       float64   `json:"rps"`
}

type AnalyzeResponse struct {
	WindowSize     int       `json:"windowSize"`
	SamplesInWin   int       `json:"samplesInWindow"`
	RollingAvgRPS  float64   `json:"rollingAvgRps"`
	MeanRPS        float64   `json:"meanRps"`
	StdDevRPS      float64   `json:"stdDevRps"`
	LastRPS        float64   `json:"lastRps"`
	ZScore         float64   `json:"zScore"`
	IsAnomaly      bool      `json:"isAnomaly"`
	AnomalyThresh  float64   `json:"anomalyThreshold"`
	TotalIngested  uint64    `json:"totalIngested"`
	TotalAnomalies uint64    `json:"totalAnomalies"`
	UpdatedAt      time.Time `json:"updatedAt"`
	Pod            string    `json:"pod"`
}

// =====================
// Rolling window (O(1))
// =====================

type rollingWindow struct {
	size  int
	buf   []float64
	idx   int
	count int
	sum   float64
	sumSq float64
}

func newRollingWindow(size int) *rollingWindow {
	return &rollingWindow{size: size, buf: make([]float64, size)}
}

func (w *rollingWindow) Add(v float64) {
	if w.size <= 0 {
		return
	}
	if w.count < w.size {
		w.buf[w.idx] = v
		w.sum += v
		w.sumSq += v * v
		w.count++
		w.idx = (w.idx + 1) % w.size
		return
	}
	old := w.buf[w.idx]
	w.buf[w.idx] = v
	w.sum += v - old
	w.sumSq += v*v - old*old
	w.idx = (w.idx + 1) % w.size
}

func (w *rollingWindow) Mean() float64 {
	if w.count == 0 {
		return 0
	}
	return w.sum / float64(w.count)
}

func (w *rollingWindow) StdDev() float64 {
	if w.count < 2 {
		return 0
	}
	mean := w.Mean()
	variance := w.sumSq/float64(w.count) - mean*mean
	if variance < 0 {
		variance = 0
	}
	return math.Sqrt(variance)
}

func (w *rollingWindow) Count() int { return w.count }

// =====================
// Minimal Prometheus metrics (no external deps)
// Exposition format: https://prometheus.io/docs/instrumenting/exposition_formats/
// =====================

type Metrics struct {
	httpRequestsTotal  atomic.Uint64
	httpErrorsTotal    atomic.Uint64
	ingestTotal         atomic.Uint64
	anomaliesTotal      atomic.Uint64
	analyzeRequests     atomic.Uint64
	healthRequests      atomic.Uint64
	metricsRequests     atomic.Uint64
	latencyCount        atomic.Uint64
	latencySumMicros    atomic.Uint64
	lastRPSBits         atomic.Uint64
	rollingAvgRPSBits   atomic.Uint64
	lastZScoreBits      atomic.Uint64
	inflightRequests    atomic.Int64
	redisSetFailures    atomic.Uint64
	redisPingFailures   atomic.Uint64
	redisSetSuccess     atomic.Uint64
	queueDropped        atomic.Uint64
	queueAccepted       atomic.Uint64
}

func setFloatBits(u *atomic.Uint64, v float64) {
	u.Store(math.Float64bits(v))
}

func getFloatBits(u *atomic.Uint64) float64 {
	return math.Float64frombits(u.Load())
}

func (m *Metrics) observeLatency(d time.Duration) {
	m.latencyCount.Add(1)
	m.latencySumMicros.Add(uint64(d.Microseconds()))
}

func (m *Metrics) render(w io.Writer) {
	// HELP/TYPE are optional but nice.
	fmt.Fprintln(w, "# HELP http_requests_total Total HTTP requests")
	fmt.Fprintln(w, "# TYPE http_requests_total counter")
	fmt.Fprintf(w, "http_requests_total %d\n", m.httpRequestsTotal.Load())

	fmt.Fprintln(w, "# HELP http_errors_total Total HTTP 5xx/4xx errors")
	fmt.Fprintln(w, "# TYPE http_errors_total counter")
	fmt.Fprintf(w, "http_errors_total %d\n", m.httpErrorsTotal.Load())

	fmt.Fprintln(w, "# HELP ingest_total Total ingested metrics")
	fmt.Fprintln(w, "# TYPE ingest_total counter")
	fmt.Fprintf(w, "ingest_total %d\n", m.ingestTotal.Load())

	fmt.Fprintln(w, "# HELP anomalies_total Total detected anomalies")
	fmt.Fprintln(w, "# TYPE anomalies_total counter")
	fmt.Fprintf(w, "anomalies_total %d\n", m.anomaliesTotal.Load())

	fmt.Fprintln(w, "# HELP last_rps Last ingested RPS")
	fmt.Fprintln(w, "# TYPE last_rps gauge")
	fmt.Fprintf(w, "last_rps %g\n", getFloatBits(&m.lastRPSBits))

	fmt.Fprintln(w, "# HELP rolling_avg_rps Rolling average RPS")
	fmt.Fprintln(w, "# TYPE rolling_avg_rps gauge")
	fmt.Fprintf(w, "rolling_avg_rps %g\n", getFloatBits(&m.rollingAvgRPSBits))

	fmt.Fprintln(w, "# HELP last_zscore Z-score of last ingested RPS")
	fmt.Fprintln(w, "# TYPE last_zscore gauge")
	fmt.Fprintf(w, "last_zscore %g\n", getFloatBits(&m.lastZScoreBits))

	fmt.Fprintln(w, "# HELP http_request_latency_micros_sum Sum of latencies in microseconds")
	fmt.Fprintln(w, "# TYPE http_request_latency_micros_sum counter")
	fmt.Fprintf(w, "http_request_latency_micros_sum %d\n", m.latencySumMicros.Load())

	fmt.Fprintln(w, "# HELP http_request_latency_count Count of observed requests")
	fmt.Fprintln(w, "# TYPE http_request_latency_count counter")
	fmt.Fprintf(w, "http_request_latency_count %d\n", m.latencyCount.Load())

	fmt.Fprintln(w, "# HELP inflight_requests In-flight HTTP requests")
	fmt.Fprintln(w, "# TYPE inflight_requests gauge")
	fmt.Fprintf(w, "inflight_requests %d\n", m.inflightRequests.Load())

	fmt.Fprintln(w, "# HELP redis_set_success_total Successful Redis SET operations")
	fmt.Fprintln(w, "# TYPE redis_set_success_total counter")
	fmt.Fprintf(w, "redis_set_success_total %d\n", m.redisSetSuccess.Load())

	fmt.Fprintln(w, "# HELP redis_set_failures_total Failed Redis SET operations")
	fmt.Fprintln(w, "# TYPE redis_set_failures_total counter")
	fmt.Fprintf(w, "redis_set_failures_total %d\n", m.redisSetFailures.Load())

	fmt.Fprintln(w, "# HELP redis_ping_failures_total Failed Redis PING operations")
	fmt.Fprintln(w, "# TYPE redis_ping_failures_total counter")
	fmt.Fprintf(w, "redis_ping_failures_total %d\n", m.redisPingFailures.Load())

	fmt.Fprintln(w, "# HELP ingest_queue_accepted_total Accepted ingest events")
	fmt.Fprintln(w, "# TYPE ingest_queue_accepted_total counter")
	fmt.Fprintf(w, "ingest_queue_accepted_total %d\n", m.queueAccepted.Load())

	fmt.Fprintln(w, "# HELP ingest_queue_dropped_total Dropped ingest events due to overload")
	fmt.Fprintln(w, "# TYPE ingest_queue_dropped_total counter")
	fmt.Fprintf(w, "ingest_queue_dropped_total %d\n", m.queueDropped.Load())
}

// =====================
// Minimal Redis client (RESP)
// =====================

type RedisClient struct {
	addr        string
	dialTimeout time.Duration
	ioTimeout   time.Duration
}

func NewRedisClient(addr string) *RedisClient {
	return &RedisClient{addr: addr, dialTimeout: 500 * time.Millisecond, ioTimeout: 500 * time.Millisecond}
}

func (c *RedisClient) Ping(ctx context.Context) error {
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.ioTimeout))

	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+PONG") {
		return fmt.Errorf("unexpected reply: %q", strings.TrimSpace(line))
	}
	return nil
}

func (c *RedisClient) SetEX(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.ioTimeout))

	ex := strconv.Itoa(ttlSeconds)
	// SET key value EX <seconds>
	payload := respArray(
		"SET",
		key,
		string(value),
		"EX",
		ex,
	)
	if _, err := conn.Write([]byte(payload)); err != nil {
		return err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+OK") {
		return fmt.Errorf("unexpected reply: %q", strings.TrimSpace(line))
	}
	return nil
}

func respArray(parts ...string) string {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(parts)))
	b.WriteString("\r\n")
	for _, p := range parts {
		b.WriteString("$")
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteString("\r\n")
		b.WriteString(p)
		b.WriteString("\r\n")
	}
	return b.String()
}

// =====================
// App
// =====================

type App struct {
	ctx    context.Context
	cancel context.CancelFunc

	id         string
	windowSize int
	zThresh    float64

	redis *RedisClient

	mu            sync.RWMutex
	win           *rollingWindow
	lastRPS       float64
	lastUpdatedAt time.Time
	totalIngest   uint64
	totalAnom     uint64

	ingestCh chan MetricIngest
	metrics  *Metrics
}

func newApp() *App {
	ctx, cancel := context.WithCancel(context.Background())

	windowSize := envInt("WINDOW_SIZE", 50)
	if windowSize < 5 {
		windowSize = 50
	}
	zThresh := envFloat("Z_THRESHOLD", 2.0)
	if zThresh <= 0 {
		zThresh = 2.0
	}

	redisAddr := getenv("REDIS_ADDR", "redis:6379")
	redis := NewRedisClient(redisAddr)

	m := &Metrics{}
	setFloatBits(&m.lastRPSBits, 0)
	setFloatBits(&m.rollingAvgRPSBits, 0)
	setFloatBits(&m.lastZScoreBits, 0)

	app := &App{
		ctx:          ctx,
		cancel:       cancel,
		id:           randomID(8),
		windowSize:   windowSize,
		zThresh:      zThresh,
		redis:        redis,
		win:          newRollingWindow(windowSize),
		ingestCh:      make(chan MetricIngest, 8192),
		metrics:      m,
		lastUpdatedAt: time.Now(),
	}

	go app.ingestWorker()
	go app.redisWarmup()
	return app
}

func (a *App) redisWarmup() {
	// Best-effort ping loop on start, doesn't block the service.
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	if err := a.redis.Ping(ctx); err != nil {
		a.metrics.redisPingFailures.Add(1)
		log.Printf("WARN: redis ping failed (%s): %v", a.redis.addr, err)
		return
	}
	log.Printf("redis ok: %s", a.redis.addr)
}

func (a *App) ingestWorker() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case m := <-a.ingestCh:
			anomaly, z, mean, std := a.processMetric(m)
			a.cacheLatest(anomaly, z, mean, std)
		}
	}
}

func (a *App) processMetric(m MetricIngest) (anomaly bool, z, mean, std float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.win.Add(m.RPS)
	mean = a.win.Mean()
	std = a.win.StdDev()
	a.lastRPS = m.RPS
	a.lastUpdatedAt = time.Now()
	a.totalIngest++

	if std > 0 {
		z = math.Abs((m.RPS - mean) / std)
	}
	anomaly = (a.win.Count() >= a.windowSize) && (z > a.zThresh)
	if anomaly {
		a.totalAnom++
		a.metrics.anomaliesTotal.Add(1)
	}

	a.metrics.ingestTotal.Add(1)
	setFloatBits(&a.metrics.lastRPSBits, m.RPS)
	setFloatBits(&a.metrics.rollingAvgRPSBits, mean)
	setFloatBits(&a.metrics.lastZScoreBits, z)

	return anomaly, z, mean, std
}

func (a *App) cacheLatest(anomaly bool, z, mean, std float64) {
	payload := map[string]any{
		"pod":       a.id,
		"updatedAt": time.Now().UTC().Format(time.RFC3339Nano),
		"rolling":   mean,
		"mean":      mean,
		"std":       std,
		"last":      a.lastRPS,
		"z":         z,
		"anomaly":   anomaly,
		"window":    a.windowSize,
		"thresh":    a.zThresh,
	}
	b, _ := json.Marshal(payload)
	key := getenv("REDIS_KEY_LATEST", "analyze:latest")

	ctx, cancel := context.WithTimeout(a.ctx, 200*time.Millisecond)
	defer cancel()
	if err := a.redis.SetEX(ctx, key, b, 30); err != nil {
		a.metrics.redisSetFailures.Add(1)
		return
	}
	a.metrics.redisSetSuccess.Add(1)
}

// =====================
// HTTP
// =====================

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.wrap("/health", a.handleHealth))
	mux.HandleFunc("/api/metrics", a.wrap("/api/metrics", a.handleIngest))
	mux.HandleFunc("/api/analyze", a.wrap("/api/analyze", a.handleAnalyze))
	mux.HandleFunc("/metrics", a.wrap("/metrics", a.handlePromMetrics))
	return mux
}

func (a *App) wrap(path string, h func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		a.metrics.inflightRequests.Add(1)
		start := time.Now()
		defer func() {
			a.metrics.inflightRequests.Add(-1)
			a.metrics.httpRequestsTotal.Add(1)
			a.metrics.observeLatency(time.Since(start))
		}()

		sw := &statusWriter{ResponseWriter: w, status: 200}
		h(sw, r)
		if sw.status >= 400 {
			a.metrics.httpErrorsTotal.Add(1)
		}
		_ = path // reserved if you want per-path metrics later
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.metrics.healthRequests.Add(1)
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "UP",
		"pod":    a.id,
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (a *App) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m, err := decodeMetric(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	select {
	case a.ingestCh <- m:
		a.metrics.queueAccepted.Add(1)
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
	default:
		a.metrics.queueDropped.Add(1)
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}
}

func (a *App) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	a.metrics.analyzeRequests.Add(1)
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.mu.RLock()
	mean := a.win.Mean()
	std := a.win.StdDev()
	last := a.lastRPS
	samples := a.win.Count()
	totalIn := a.totalIngest
	totalAn := a.totalAnom
	updated := a.lastUpdatedAt
	a.mu.RUnlock()

	z := 0.0
	if std > 0 {
		z = math.Abs((last - mean) / std)
	}
	isAnom := (samples >= a.windowSize) && (z > a.zThresh)

	resp := AnalyzeResponse{
		WindowSize:     a.windowSize,
		SamplesInWin:   samples,
		RollingAvgRPS:  mean,
		MeanRPS:        mean,
		StdDevRPS:      std,
		LastRPS:        last,
		ZScore:         z,
		IsAnomaly:      isAnom,
		AnomalyThresh:  a.zThresh,
		TotalIngested:  totalIn,
		TotalAnomalies: totalAn,
		UpdatedAt:      updated,
		Pod:            a.id,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *App) handlePromMetrics(w http.ResponseWriter, r *http.Request) {
	a.metrics.metricsRequests.Add(1)
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	a.metrics.render(w)
}

func decodeMetric(r *http.Request) (MetricIngest, error) {
	var raw map[string]any
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return MetricIngest{}, fmt.Errorf("invalid json: %w", err)
	}

	getF := func(k string) (float64, error) {
		v, ok := raw[k]
		if !ok {
			return 0, fmt.Errorf("missing field %q", k)
		}
		switch t := v.(type) {
		case float64:
			return t, nil
		case int:
			return float64(t), nil
		default:
			return 0, fmt.Errorf("field %q must be number", k)
		}
	}

	cpu, err := getF("cpu")
	if err != nil {
		return MetricIngest{}, err
	}
	rps, err := getF("rps")
	if err != nil {
		return MetricIngest{}, err
	}
	if cpu < 0 || cpu > 100 {
		return MetricIngest{}, errors.New("cpu must be within 0..100")
	}
	if rps < 0 {
		return MetricIngest{}, errors.New("rps must be >= 0")
	}

	var ts time.Time
	if v, ok := raw["timestamp"]; ok {
		s, ok := v.(string)
		if !ok {
			return MetricIngest{}, errors.New("timestamp must be RFC3339 string")
		}
		parsed, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, s)
			if err != nil {
				return MetricIngest{}, errors.New("timestamp must be RFC3339 or RFC3339Nano")
			}
		}
		ts = parsed
	} else {
		ts = time.Now().UTC()
	}

	return MetricIngest{Timestamp: ts, CPU: cpu, RPS: rps}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// =====================
// Utils
// =====================

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func randomID(bytesLen int) string {
	b := make([]byte, bytesLen)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// =====================
// main
// =====================

func main() {
	app := newApp()
	port := getenv("PORT", "8080")

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           app.routes(),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("starting :%s (pod=%s, window=%d, z>%.2f, redis=%s)", port, app.id, app.windowSize, app.zThresh, app.redis.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("shutting down...")
	app.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Printf("bye")
}
