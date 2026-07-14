// Package taskkit is a small, zero-dependency SDK for developers writing task binaries.
// It wires log/slog to stdout and provides helpers to read task payload from stdin
// and write the result to FD3.
package taskkit

import (
	"encoding/json"
	"fmt"
	"os"
)

// ReadInput reads and JSON-decodes the task payload from stdin into v.
func ReadInput(v any) error {
	if err := json.NewDecoder(os.Stdin).Decode(v); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

// MustReadInput calls ReadInput and panics on error.
// On panic the worker captures stderr and sends TYPE_JOB_ERROR to the Manager.
func MustReadInput(v any) {
	if err := ReadInput(v); err != nil {
		panic(err)
	}
}
