package taskkit

import "os"

// Phase represents the execution phase of a job binary.
type Phase string

const (
	// PhaseRun is the default phase. The binary runs the actual work and either
	// submits a result via SubmitResult or emits child jobs via EmitJobs.
	PhaseRun Phase = "run"

	// PhaseConsolidate is triggered after all child jobs have completed.
	// The binary receives the consolidated input/children results and should
	// produce a final result via SubmitResult.
	PhaseConsolidate Phase = "consolidate"
)

// CurrentPhase returns the current execution phase set by the Worker.
// Returns PhaseRun when WORKFORCE_PHASE is unset (e.g. running in a terminal).
func CurrentPhase() Phase {
	if p := os.Getenv(EnvPhase); p == string(PhaseConsolidate) {
		return PhaseConsolidate
	}

	return PhaseRun
}
