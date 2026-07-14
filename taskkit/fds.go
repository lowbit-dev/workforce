package taskkit

// FDs holds the configuration for the special file descriptors used by SubmitResult
// and EmitJobs.
//
// The Worker opens these pipes before exec-ing a task binary and the binary
// inherits them at the configured fd numbers. Task binaries do not need to
// change the default fd numbers unless running in a non-standard environment.
var FDs = struct {
	// ResultFD is the file-descriptor number that SubmitResult writes to. Default: 3.
	ResultFD int
	// SubjobsFD is the file-descriptor number that EmitJobs writes to. Default: 4.
	SubjobsFD int
}{
	ResultFD:  3,
	SubjobsFD: 4,
}
