package worker

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"lowbit.dev/cooper"
	"lowbit.dev/netargv"
	"lowbit.dev/retry"
	"lowbit.dev/rungroup"
	"lowbit.dev/verreg"
	"lowbit.dev/workforce/contract"
	"lowbit.dev/workforce/worker/cache"
)

var (
	ErrHandshakeFailed = errors.New("handshake failed")
)

// ResourceLimits defines system-level safety margins for the Worker.
// All percentages are in the range (0, 100]. Zero means the limit is not enforced.
type ResourceLimits struct {
	// MaxCPUPercent is the maximum whole-machine CPU utilisation at which the worker
	// will accept a new task. E.g. 80.0 means "NACK if the machine CPU is already above 80%".
	// Default: 0 (no enforcement).
	MaxCPUPercent float64

	// MaxMemPercent is the equivalent ceiling for whole-machine memory utilisation.
	// Default: 0 (no enforcement).
	MaxMemPercent float64

	// SampleInterval controls how often the background sampler refreshes the cached metrics.
	// Shorter = more responsive; longer = less syscall overhead. Default: 5s.
	SampleInterval time.Duration
}

// Config holds all configuration for the Worker daemon.
type Config struct {
	// ManagerAddr is the address the worker can connect to to obtain a persisted connection, e.g. "http://manager:8080/worker/connect".
	ManagerAddr string

	// Capacity is the total capacity units this node declares.
	Capacity int

	// CacheDir is the local path for cached binaries. Default: /tmp/workforce.
	CacheDir string

	// CacheMaxSize is the maximum total bytes the binary cache may occupy. Zero = no size limit.
	CacheMaxSize int64

	// CacheTTL evicts binaries not accessed within this window. Zero = no TTL eviction.
	CacheTTL time.Duration

	// TmpStorageDir is the path where the worker can manage tmp storage which should not be supervised by the kernel.
	// This storage may be shared by tasks on the worker or be single task scoped.
	// The worker will however enforce the TmpStorageMaxLifetime
	TmpStorageDir string

	// TmpStorageMaxLifetime defines the maximum lifetime of a file or directory stored in the TmpStorageDirectory. (Default 48 * Hour)
	// Once a file or directort exeeds this lifetime the worker will unlink it.
	TmpStorageMaxLifetime time.Duration

	// TmpStorageCleanupInterval defines the interval in which the tmp storage locatioon will be cleaned up (Default: 3*Hour)
	TmpStorageCleanupInterval time.Duration

	// HeartbeatInterval governs how often the Worker sends TYPE_HEARTBEAT. Default: 10s.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long the Worker waits for a heartbeat response before declaring
	// the Manager dead and triggering reconnect. Default: 30s.
	HeartbeatTimeout time.Duration

	// WorkerID is an optional stable identifier for this node. Generated if empty.
	WorkerID string

	// AuthToken is sent as a Bearer token in the Authorization header of POST /workers/connect.
	AuthToken string

	// InheritableENVPrefix defines the prefix for ENV variables which are inherited and passed to
	// the task binaries ran through the worker
	InheritableENVPrefix string

	// ReconnectMaxAttempts is the maximum number of reconnect attempts. (Defaults to 10000).
	ReconnectMaxAttempts int

	// ConnectionRetryPolicy is the retry policy which is applied to the main connect and work loop
	ConnectionRetryStrategy retry.Strategy

	// KillGracePeriod is the time between SIGTERM and SIGKILL when MaxExecutionTime is exceeded.
	// Default: 5s.
	KillGracePeriod time.Duration

	// AllowNonRoot allows the Worker to start without root privileges.
	// When false (default), resource limit enforcement (cgroup/RLIMIT) requires root.
	// When true, if enforcement writes fail the Worker logs a warning and continues.
	AllowNonRoot bool

	// ResourceLimits configures system-level safety margins.
	// When non-zero, the worker will NACK proposals that would push the machine past the
	// configured thresholds, even if declared capacity is still available.
	ResourceLimits ResourceLimits

	// Logger is used for all internal Worker log output. Defaults to slog.Default() if nil.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}

	if c.CacheDir == "" {
		c.CacheDir = "/tmp/workforce"
	}

	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 10 * time.Second
	}

	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 30 * time.Second
	}

	if c.KillGracePeriod == 0 {
		c.KillGracePeriod = 5 * time.Second
	}

	if c.Capacity == 0 {
		c.Capacity = 1
	}

	if c.ResourceLimits.SampleInterval == 0 {
		c.ResourceLimits.SampleInterval = 2 * time.Second
	}

	if c.ReconnectMaxAttempts == 0 {
		c.ReconnectMaxAttempts = 10000 // Limit it to some finite number to not create infinite retry loops
	}

	if c.ConnectionRetryStrategy == nil {
		c.ConnectionRetryStrategy = retry.Exponential(5*time.Second, 10*time.Minute)
	}

	if c.InheritableENVPrefix == "" {
		c.InheritableENVPrefix = "WORKFORCE_WORKER_TASK_"
	}

	if c.TmpStorageMaxLifetime == 0 {
		c.TmpStorageMaxLifetime = 48 * time.Hour
	}

	if c.TmpStorageCleanupInterval == 0 {
		c.TmpStorageCleanupInterval = 3 * time.Hour
	}
}

// Worker connects to a Manager and executes task binaries.
type Worker struct {
	// Persistent — survive reconnects.
	cfg Config

	artifactCache         *cache.ArtifactCache
	metricsSampler        *metricsSampler
	connectionRetryPolicy retry.Strategy

	messageVerreg *verreg.Registry[contract.MessageFactory]

	// depCache holds executables confirmed present in PATH.
	// Only positive hits are cached; misses are always re-checked via LookPath
	// so a dep installed after startup is picked up on the next proposal.
	depCacheMu sync.RWMutex
	depCache   map[string]struct{}

	// Connection-scoped — reset at the start of each connectAndServe.
	state contract.AtomicWorkerState
	conn  net.Conn
	done  chan struct{} // closed on planned exit (drain/shutdown)

	tasksMu                   sync.Mutex
	tasks                     map[string]*activeTask
	pendingJobs               map[string]*contract.ProposeMessage // accepted, awaiting dispatch; protected by tasksMu
	usedCap                   int                                 // sum of costs of all running tasks; protected by tasksMu
	lastHeartbeatAck          atomic.Int64                        // unix nanos of last heartbeat echo
	heartbeatSendFaulureCount atomic.Int32
	sendMu                    sync.Mutex
}

// activeTask tracks one running job binary.
type activeTask struct {
	cancel context.CancelFunc
	cost   int    // capacity units consumed; returned to the pool in taskFinished
	runID  string // run identifier echoed from DispatchMessage
}

// New validates cfg, applies defaults, and returns a ready Worker.
func New(cfg Config) (*Worker, error) {
	cfg.applyDefaults()

	c, err := cache.NewArtifactCache(cfg.CacheDir, cfg.CacheMaxSize, cfg.CacheTTL, cfg.Logger)
	if err != nil {
		return nil, err
	}

	r := verreg.NewRegistry[contract.MessageFactory]()
	contract.RegisterMessages(r)
	r.Build()

	w := &Worker{
		cfg:            cfg,
		artifactCache:  c,
		metricsSampler: newMetricsSampler(cfg.ResourceLimits.SampleInterval, cfg.ResourceLimits.SampleInterval/2, cfg.Logger),

		messageVerreg: r,
		depCache:      make(map[string]struct{}),
		state:         contract.AtomicWorkerState{},
	}

	w.state.Store(contract.WorkerStateOffline)

	return w, nil
}

// Run connects to the Manager and processes tasks until ctx is cancelled or the
// Manager requests a planned exit. Returns a non-nil error only when
// ReconnectMaxAttempts is exhausted.
func (w *Worker) Run(ctx context.Context) error {
	if w.cfg.WorkerID == "" {
		w.cfg.WorkerID = generateID()
	}

	rg := rungroup.New(
		rungroup.WithShutdownBoundary(),
		rungroup.WithShutdownTimeout(time.Minute*15),
		rungroup.WithEventHandler(func(e rungroup.Event) {
			slog.Info("[Worker][RunGroup] Event Received", "event", e)
		}),
	)

	rg.Add(rungroup.NewIntervalService(w.cfg.HeartbeatInterval, w.HeartbeatRoutine),
		rungroup.WithName("HeartbeatRoutine"),
		rungroup.WithBackoff(retry.Exponential(30*time.Second, 5*time.Minute)),
		rungroup.WithStabilityWindow(time.Minute*5),
	)

	if w.cfg.ResourceLimits.MaxCPUPercent > 0 || w.cfg.ResourceLimits.MaxMemPercent > 0 {
		rg.Add(w.metricsSampler,
			rungroup.WithName("MetricsSamplerRoutine"),
			rungroup.WithBackoff(retry.MaxAttempts(10, retry.Exponential(30*time.Second, 10*time.Minute))),
			rungroup.WithStabilityWindow(time.Minute*5),
		)
	}

	rg.Add(rungroup.ServiceFunc(w.ConnectAndWorkRoutine),
		rungroup.WithName("ConnectionHandlerRoutine"),
		rungroup.WithBackoff(retry.MaxAttempts(w.cfg.ReconnectMaxAttempts, w.cfg.ConnectionRetryStrategy)),
		rungroup.WithStabilityWindow(time.Minute*5),
	)

	return rg.Run(ctx)
}

func (w *Worker) ConnectAndWorkRoutine(ctx context.Context) error {
	err := w.ConnectAndWork(ctx)
	slog.Error("[ConnectAndWorkRoutine] Error while performing work", "error", err)

	if w.state.Is(contract.WorkerStateShuttingDown) || ctx.Err() != nil || err == nil {
		w.cfg.Logger.Info("[ConnectAndWorkRoutine] Received Shutdown signal. Shutting down...")
		return fmt.Errorf("%w: %w", rungroup.ErrShutdownAll, err)
	}

	if errors.Is(err, retry.ErrRetryLimitExceeded) {
		w.cfg.Logger.Info("[ConnectAndWorkRoutine] Max reconnect attempts exhausted. Shutting down...")
		return fmt.Errorf("%w: %w", rungroup.ErrShutdownAll, err)
	}

	return err
}

// run attempts one connection. On an unexpected disconnect it backs off and calls itself
// with attempt+1. On a planned exit or context cancellation it returns nil.
func (w *Worker) ConnectAndWork(ctx context.Context) error {
	w.state.Store(contract.WorkerStateOffline)
	w.done = make(chan struct{})
	w.tasks = make(map[string]*activeTask)
	w.pendingJobs = make(map[string]*contract.ProposeMessage)
	w.usedCap = 0

	req, err := http.NewRequest(http.MethodPost, w.cfg.ManagerAddr, nil)
	if err != nil {
		return fmt.Errorf("failed to build connection request: %w", err)
	}

	workerKey := contract.GenerateWorkforceKey()
	slog.Info("[Worker][ConnectAndWork] Generated worker key for handshake", "key", workerKey)

	req.Header.Set(contract.ProtoMagicKeyHeaderName, workerKey)
	req.Header.Set("X-Worker-ID", w.cfg.WorkerID)
	req.Header.Set("X-Worker-Capacity", fmt.Sprint(w.cfg.Capacity))
	req.Header.Set("X-Worker-OS", runtime.GOOS)
	req.Header.Set("X-Worker-Arch", runtime.GOARCH)

	if w.cfg.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+w.cfg.AuthToken)
	}

	worforceProtocolVersion := verreg.Version(0)
	dialOpts := []cooper.DialOption{
		cooper.WithProtocol(fmt.Sprintf("workforce/%d", worforceProtocolVersion)),
		cooper.WithUpgradeOptions(contract.ProtoResponseValidator(workerKey)),
	}

	if req.URL.Scheme == "https" {
		dialOpts = append(dialOpts, cooper.WithTLSConfig(&tls.Config{ServerName: req.URL.Hostname()}))
	}
	w.conn, err = cooper.Dial(req, dialOpts...)

	if err != nil {
		return fmt.Errorf("failed to connect to manager: %w", err)
	}

	defer w.conn.Close()
	defer func() { w.conn = nil }()

	w.lastHeartbeatAck.Store(time.Now().UnixNano())
	w.state.Store(contract.WorkerStateOnline)

	reader := netargv.NewReader(w.conn)
	for msg, err := range reader.Itterate(ctx) {
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("failed to read message from stream: %w", err)
		}

		factory, err := w.messageVerreg.Resolve(worforceProtocolVersion, msg.Verb())
		if err != nil {
			w.sendError("unknown_verb", "unknown message verb: "+msg.Verb())
			continue
		}

		message, err := factory(msg)
		if err != nil {
			w.sendError("invalid_message", err.Error())
			continue
		}

		go w.handleMessage(ctx, message)
	}

	return nil
}

func (w *Worker) HeartbeatRoutine(ctx context.Context) error {
	if w.conn == nil {
		slog.Debug("[HeartbeatRoutine] No connection established yet. Skipping round...")
		return nil
	}

	delta := time.Now().Unix() - w.lastHeartbeatAck.Load()
	if delta > int64(w.cfg.HeartbeatTimeout.Seconds()) {
		w.cfg.Logger.Warn("[Worker][HeartbeatRoutine] Heartbeat timed-out, did not recieve heartbeat from manager within timout window. Closing connection...", "worker_id", w.cfg.WorkerID, "delta", delta)
		_ = w.conn.Close()

		return rungroup.ErrShutdownAll
	}

	w.cfg.Logger.Debug("[HeartbeatRoutine] Obtaining metrics snapshot")
	metrics := w.metricsSampler.snapshot()

	if w.state.Is(contract.WorkerStateOnline) || w.state.Is(contract.WorkerStatePressure) {
		if metrics.IsOverLimit(w.cfg.ResourceLimits.MaxCPUPercent, w.cfg.ResourceLimits.MaxMemPercent) {
			w.state.Store(contract.WorkerStatePressure)
		} else {
			w.state.Store(contract.WorkerStateOnline)
		}
	}

	msg := fmt.Sprintf("heartbeat --cpu=%.2f --mem=%.2f --state=%d", metrics.CPUPercent, metrics.MemPercent, w.state.Load())
	if err := w.send(msg); err != nil {
		w.cfg.Logger.Warn("[Worker][HeartbeatRoutine] Failed to send heartbeat", "error", err)
		totalConsecutiveFaulures := w.heartbeatSendFaulureCount.Add(1)

		if totalConsecutiveFaulures >= 3 {
			w.cfg.Logger.Warn("[Worker][HeartbeatRoutine] Failed to send heartbeat 3 times in a row. Colsing Connection...", "error", err)
			_ = w.conn.Close()
		}

		return err
	}

	if w.heartbeatSendFaulureCount.Load() > 0 {
		w.heartbeatSendFaulureCount.Add(-1)
	}

	return nil
}

// checkDeps checks that every executable in deps is present in PATH.
// Confirmed executables are cached; misses are always re-checked so a dep
// installed after startup is found on the next proposal.
// Returns the names of any executables that could not be located.
func (w *Worker) checkDeps(deps []string) []string {
	if len(deps) == 0 {
		return nil
	}

	var missing []string
	for _, dep := range deps {
		w.depCacheMu.RLock()
		_, cached := w.depCache[dep]
		w.depCacheMu.RUnlock()

		if cached {
			continue
		}

		if _, err := exec.LookPath(dep); err != nil {
			missing = append(missing, dep)
			continue
		}

		w.depCacheMu.Lock()
		w.depCache[dep] = struct{}{}
		w.depCacheMu.Unlock()
	}

	return missing
}

// ---- helpers ----

// send writes a pre-formatted netargv line to the connection.
func (w *Worker) send(line string) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	_, err := fmt.Fprintln(w.conn, line)
	return err
}

// sendWithPayload writes a netargv message with a binary payload.
// header must be a fully formed netargv header without the "-- <n>" suffix.
// The payload length and bytes are appended atomically under sendMu.
func (w *Worker) sendWithPayload(header string, payload []byte) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if _, err := fmt.Fprintf(w.conn, "%s -- %d\n", header, len(payload)); err != nil {
		return err
	}
	_, err := w.conn.Write(payload)
	return err
}

// generateID returns a random worker ID incorporating the hostname.
func generateID() string {
	host, _ := osHostname()
	return host + "-" + shortID()
}

// shortID generates a cryptographically random 8-byte (16 hex char) ID.
func shortID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate random ID: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// osHostname is a thin wrapper so it can be overridden in tests.
var osHostname = func() (string, error) {
	return runtime.GOOS + "-worker", nil
}
