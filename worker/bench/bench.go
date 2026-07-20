package bench

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// Config holds the baseline metrics.
// If BaselineCPUOpsPerSec is 0, the package runs in Calibration Mode.
type Config struct {
	BaselineCPUOpsPerSec  int           `unit:"ops/sec"`
	BaselineMemBandwidth  int           `unit:"MB/s"`
	BaselineDiskBandwidth int           `unit:"MB/s"`
	BenchDuration         time.Duration `unit:"duration"`

	// Weight distribution (must add up to 1.0). If left at 0, defaults to 0.70 / 0.30.
	CPUWeight  float64 `unit:"weight"`
	MemWeight  float64 `unit:"weight"`
	DiskWeight float64 `unit:"weight"`
}

// Result holds both raw and normalized performance metrics.
type Result struct {
	Cores            int     `unit:"cores"`
	RawOpsPerSec     int     `unit:"ops/sec"`
	RawMemBandwidth  int     `unit:"MB/s"`
	CPUMultiplier    float64 `unit:"x"`
	MemMultiplier    float64 `unit:"x"`
	DiskMultiplier   float64 `unit:"x"`
	RawDiskBandwidth int     `unit:"MB/s"`
	TotalWUs         int     `unit:"WUs"`
}

// Run executes the synthetic benchmarks and returns raw + calculated metrics.
func Run(cfg Config) Result {
	if cfg.BenchDuration == 0 {
		cfg.BenchDuration = 2 * time.Second
	}

	// Apply default weights if none are provided
	if cfg.CPUWeight == 0 && cfg.MemWeight == 0 && cfg.DiskWeight == 0 {
		cfg.CPUWeight = 0.60
		cfg.MemWeight = 0.25
		cfg.DiskWeight = 0.15
	}

	// 1. Run Benchmarks
	rawOps := benchmarkCPU(cfg.BenchDuration)
	rawMemBandwidth := benchmarkMemory()
	rawDiskBandwidth := benchmarkDisk()

	cores := runtime.NumCPU()
	var cpuMultiplier float64
	var memMultiplier float64
	var diskMultiplier float64
	var totalWUs int

	// 2. Calculate performance multipliers
	if cfg.BaselineCPUOpsPerSec > 0 {
		cpuMultiplier = float64(rawOps) / float64(cfg.BaselineCPUOpsPerSec)
	}
	if cfg.BaselineMemBandwidth > 0 {
		memMultiplier = float64(rawMemBandwidth) / float64(cfg.BaselineMemBandwidth)
	}
	if cfg.BaselineDiskBandwidth > 0 {
		diskMultiplier = float64(rawDiskBandwidth) / float64(cfg.BaselineDiskBandwidth)
	}

	// 3. Calculate Unified Work Units (WUs)
	if cfg.BaselineCPUOpsPerSec > 0 && cfg.BaselineMemBandwidth > 0 && cfg.BaselineDiskBandwidth > 0 {
		cpuPart := float64(cores) * cpuMultiplier * (cfg.CPUWeight * 100)
		memPart := memMultiplier * (cfg.MemWeight * 100)
		diskPart := diskMultiplier * (cfg.DiskWeight * 100)
		totalWUs = int(cpuPart + memPart + diskPart)
	} else {
		// Fallback for calibration/development runs
		totalWUs = 100
	}

	return Result{
		Cores:            cores,
		RawOpsPerSec:     rawOps,
		RawMemBandwidth:  rawMemBandwidth,
		RawDiskBandwidth: rawDiskBandwidth,
		CPUMultiplier:    cpuMultiplier,
		MemMultiplier:    memMultiplier,
		DiskMultiplier:   diskMultiplier,
		TotalWUs:         totalWUs,
	}
}

func benchmarkCPU(duration time.Duration) int {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	data := []byte("benchmarking-workforce-worker-capacity")
	count := 0

	start := time.Now()
	for time.Since(start) < duration {
		h := sha256.New()
		h.Write(data)
		_ = h.Sum(nil)
		count++
	}

	elapsed := time.Since(start).Seconds()
	return int(float64(count) / elapsed)
}

func benchmarkMemory() int {
	// 128MB is large enough to bypass CPU L1/L2/L3 caches
	const sizeBytes = 128 * 1024 * 1024
	const sizeMB = 128.0

	block := make([]byte, sizeBytes)

	// WARM-UP PHASE: Touch 1 byte per page.
	// This forces the OS to allocate physical memory pages BEFORE we start the timer.
	for i := 0; i < sizeBytes; i += 4096 {
		block[i] = 1
	}

	// START TIMER
	start := time.Now()

	// 1. Sequential Write (touching every single byte)
	for i := 0; i < sizeBytes; i++ {
		block[i] = byte(i)
	}

	// 2. Sequential Read (touching every single byte)
	sum := byte(0)
	for i := 0; i < sizeBytes; i++ {
		sum += block[i]
	}

	elapsed := time.Since(start).Seconds()

	// Prevent the compiler from optimizing away the loop and sum
	runtime.KeepAlive(sum)
	block = nil

	// Aggressive GC & OS Memory release
	runtime.GC()
	debug.FreeOSMemory()

	// Total data processed is 128MB (written) + 128MB (read) = 256MB
	totalMB := sizeMB * 2

	return int(totalMB / elapsed)
}

// benchmarkDisk measures pure sequential write throughput utilizing fsync.
func benchmarkDisk() int {
	const totalSize = 64 * 1024 * 1024 // 64MB File
	const chunkSize = 1024 * 1024      // 1MB chunks

	// Create temporary file path
	tempDir := os.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "nodebench-disk-*")
	if err != nil {
		return 0 // Return 0 rather than panicking if we hit a permission/space issue
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Fill a buffer with arbitrary chunk data
	buffer := make([]byte, chunkSize)
	_, _ = rand.Read(buffer)

	start := time.Now()

	// Write 64MB sequentially
	for i := 0; i < totalSize; i += chunkSize {
		if _, err := tempFile.Write(buffer); err != nil {
			return 0
		}
	}

	// FORCE operating system to physically flush the cache buffer down to disk
	if err := tempFile.Sync(); err != nil {
		return 0
	}

	elapsed := time.Since(start).Seconds()

	mbWritten := float64(totalSize) / (1024 * 1024)
	return int(mbWritten / elapsed)
}

// PrintTable prints any struct (like Config or Result) as a beautifully aligned,
// bordered ASCII table using slog.Info. It utilizes the "unit" struct tag.
func PrintTable(data any) {
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)

	if t.Kind() == reflect.Pointer {
		t = t.Elem()
		v = v.Elem()
	}

	if t.Kind() != reflect.Struct {
		slog.Error("PrintTable: provided data is not a struct or a pointer to a struct")
		return
	}

	maxFieldLen := 0
	maxUnitLen := 0
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if len(field.Name) > maxFieldLen {
			maxFieldLen = len(field.Name)
		}
		if unit := field.Tag.Get("unit"); len(unit) > maxUnitLen {
			maxUnitLen = len(unit)
		}
	}

	unitColWidth := 0
	if maxUnitLen > 0 {
		unitColWidth = maxUnitLen + 2 // +2 for ( and )
	}

	// Calculate dynamic maximum value string length to align the border perfectly
	maxValLen := 0
	for i := 0; i < t.NumField(); i++ {
		val := v.Field(i).Interface()
		// Clean up floating point precision for cleaner reading
		if f, ok := val.(float64); ok {
			val = fmt.Sprintf("%.2f", f)
		}
		valStr := fmt.Sprintf("%v", val)
		if len(valStr) > maxValLen {
			maxValLen = len(valStr)
		}
	}

	// Calculate row layout spacing: "│  " + field + "  " + unitCol + "  =  "
	var rowPrefixLen int
	if unitColWidth > 0 {
		rowPrefixLen = 3 + maxFieldLen + 2 + unitColWidth + 5
	} else {
		rowPrefixLen = 3 + maxFieldLen + 5
	}
	totalWidth := rowPrefixLen + maxValLen

	if headerLen := 3 + len(t.Name()); headerLen > totalWidth {
		totalWidth = headerLen + 5
	}

	topLine := "┌ " + t.Name() + " " + strings.Repeat("─", totalWidth-3-len(t.Name()))
	bottomLine := "└" + strings.Repeat("─", totalWidth-1)

	slog.Info(topLine)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		unitTag := field.Tag.Get("unit")

		unitPart := ""
		if unitColWidth > 0 {
			if unitTag != "" {
				unitPart = fmt.Sprintf("(%s)", unitTag)
			}
			unitPart = fmt.Sprintf("%-*s", unitColWidth, unitPart)
		}

		val := v.Field(i).Interface()
		if f, ok := val.(float64); ok {
			val = fmt.Sprintf("%.2f", f)
		}

		if unitColWidth > 0 {
			slog.Info(fmt.Sprintf("│  %-*s  %s  =  %v", maxFieldLen, field.Name, unitPart, val))
		} else {
			slog.Info(fmt.Sprintf("│  %-*s  =  %v", maxFieldLen, field.Name, val))
		}
	}

	slog.Info(bottomLine)
}

// ScaleWUs multiplies the total WUs by a float64 multiplier
// and rounds the result down to the nearest integer.
func ScaleWUs(totalWUs int, multiplier float64) int {
	return int(math.Floor(float64(totalWUs) * multiplier))
}
