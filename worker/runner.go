package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// taskExec holds the parameters for a single task binary execution.
type taskExec struct {
	JobID       string
	TaskName    string
	ParentJobID string
	Phase       string
	Attempt     int
	BinaryPath  string
	Payload     []byte
	Limits      Limitations
	Proc        ProcConfig
	NoResult    bool
}

// ProcConfig controls how the task process is launched.
type ProcConfig struct {
	// Credential sets the user and group identity for the process.
	// Requires the Worker to have sufficient privileges to switch identity.
	Credential *syscall.Credential
	// RootDir is the working directory for the task binary.
	// If empty, a temporary directory is created and removed after the task exits.
	RootDir string
	// Env is a list of additional environment variables in KEY=VALUE form
	// to pass to the task binary. The WORKFORCE_* variables are always set.
	// The parent process environment is never inherited.
	Env []string
}

// Limitations defines the resource and execution constraints the Worker enforces on the task binary.
// Zero values mean no limit is applied for that field.
type Limitations struct {
	// MaxExecutionTime is the maximum wall-clock time the binary may run.
	// Zero means no time limit.
	MaxExecutionTime time.Duration
	// MaxMemoryBytes is the maximum resident memory the process may use (bytes).
	// Enforced via RLIMIT_AS on Linux. Zero means no limit.
	MaxMemoryBytes int64
	// MaxCPUCores is the maximum number of CPU cores the process may use.
	// Enforced via cgroup cpu.max on Linux. Zero means no limit.
	MaxCPUCores float64
}

// runTask executes t.BinaryPath as a subprocess and returns its output.
// Stdout is written to logOutput; pass io.Discard to suppress it.
// FD3 carries the result payload; FD4 carries child job JSON.
func (w *Worker) RunTask(ctx context.Context, t taskExec, logOutput io.Writer) (result, childJobs []byte, stderr string, err error) {
	runCtx := ctx
	cancelTimeout := func() {}

	if t.Limits.MaxExecutionTime > 0 {
		runCtx, cancelTimeout = context.WithTimeout(ctx, t.Limits.MaxExecutionTime)
	}

	defer cancelTimeout()

	resultR, resultW, err := os.Pipe()
	if err != nil {
		return nil, nil, "", fmt.Errorf("runner: result pipe: %w", err)
	}
	defer resultR.Close()

	childJobsR, childJobsW, err := os.Pipe()
	if err != nil {
		resultR.Close()
		resultW.Close()
		return nil, nil, "", fmt.Errorf("runner: child jobs pipe: %w", err)
	}
	defer childJobsR.Close()

	rootDir := t.Proc.RootDir
	if rootDir == "" {
		rootDir, err = os.MkdirTemp("", "workforce-task-*")
		if err != nil {
			resultR.Close()
			resultW.Close()
			childJobsR.Close()
			childJobsW.Close()
			return nil, nil, "", fmt.Errorf("runner: create temp root: %w", err)
		}
		defer os.RemoveAll(rootDir)
	}

	cmd := exec.CommandContext(runCtx, t.BinaryPath)
	cmd.Stdin = bytes.NewReader(t.Payload)
	cmd.Dir = rootDir
	cmd.ExtraFiles = []*os.File{resultW, childJobsW} // FD3=result, FD4=child jobs
	cmd.Env = append(t.Proc.Env,
		"WORKFORCE_PHASE="+t.Phase,
		"WORKFORCE_JOB_ID="+t.JobID,
		"WORKFORCE_TASK_TYPE="+t.TaskName,
		"WORKFORCE_PARENT_JOB_ID="+t.ParentJobID,
		fmt.Sprintf("WORKFORCE_ATTEMPT=%d", t.Attempt),

		fmt.Sprintf("WORKFORCE_TMP_STORAGE_DIRECTORY=%s", w.cfg.TmpStorageDir),
		fmt.Sprintf("WORKFORCE_TASK_STORAGE_DIRECTORY=%s", rootDir),
	)

	if t.Proc.Credential != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Credential: t.Proc.Credential,
		}
	}

	cmd.Stdout = logOutput

	var stderrBuf bytes.Buffer
	cmd.Stderr = io.MultiWriter(&stderrBuf, logOutput)

	// TODO: Enable apply limits

	if err := cmd.Start(); err != nil {
		resultW.Close()
		childJobsW.Close()
		return nil, nil, "", fmt.Errorf("runner: start: %w", err)
	}

	resultW.Close()
	childJobsW.Close()

	resultCh := make(chan []byte, 1)
	childJobsCh := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(resultR); resultCh <- data }()
	go func() { data, _ := io.ReadAll(childJobsR); childJobsCh <- data }()

	waitErr := cmd.Wait()
	resultData := <-resultCh
	childJobsData := <-childJobsCh

	if code := exitCodeOf(waitErr); code != 0 {
		reason := stderrBuf.String()
		if reason == "" {
			reason = waitErr.Error()
		}
		return nil, nil, "", &exitError{code: code, reason: reason}
	}

	if !t.NoResult && len(childJobsData) == 0 && len(resultData) == 0 {
		return nil, nil, "", fmt.Errorf("binary exited 0 without writing a result or child jobs")
	}

	return resultData, childJobsData, stderrBuf.String(), nil
}

// exitCodeOf returns the process exit code from a cmd.Wait error, or 0 on success.
func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}

	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}

	return 1
}

// exitError is returned when the task binary exits with a non-zero code.
type exitError struct {
	code   int
	reason string
}

func (e *exitError) Error() string { return fmt.Sprintf("exit %d: %s", e.code, e.reason) }

// IsExitError reports whether err is a non-zero binary exit.
func IsExitError(err error) (code int, reason string, ok bool) {
	if ee, is := err.(*exitError); is {
		return ee.code, ee.reason, true
	}
	return 0, "", false
}

// systemEnv returns the bare-minimum system environment variables needed by most
// task binaries. Only variables that are non-empty on the host are included.
// TMPDIR is intentionally excluded; callers should set it to the task's rootDir.
func SystemEnv() []string {
	keys := []string{
		"PATH",
		"HOME",
		"SSL_CERT_FILE",
		"SSL_CERT_DIR",
		// Windows stuff
		"COMSPEC",
		"SYSTEMROOT",
		"USERPROFILE",
		"HOMEDRIVE",
		"HOMEPATH",
		"TMP",
		"TEMP",
	}

	env := make([]string, 0, len(keys))
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			env = append(env, k+"="+v)
		}
	}

	return env
}

func EnvWithPrefix(prefix string) []string {
	var result []string

	for _, env := range os.Environ() {
		if strings.HasPrefix(env, prefix) {
			result = append(result, env)
		}
	}

	return result
}
