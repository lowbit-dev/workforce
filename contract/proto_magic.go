package contract

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"

	"lowbit.dev/cooper"
	"lowbit.dev/retry"
	"lowbit.dev/rungroup"
)

const ProtoUpgradeMagic = "mBNo4mcokFdK2qcUoT687KIPwD41UqME"
const ProtoMagicKeyHeaderName = "X-Workforce-Key"
const ProtoMagicAcceptHeaderName = "X-Workforce-Accept"

var (
	ErrInvalidWorkforceAccept = errors.New("invalid " + ProtoMagicAcceptHeaderName + " value")
	ProtoResponseValidator    = func(workerKey string) cooper.UpgradeOption {
		return cooper.ResponseValidator(func(r *http.Request, resp *http.Response) error {
			if !ValidateWorkforceAccept(workerKey, resp.Header.Get(ProtoMagicAcceptHeaderName)) {
				expected := GenerateWorkforceAccept(workerKey)

				slog.Info("[ResponseValidator][ValidateWorkforceAccept] Invalid "+ProtoMagicAcceptHeaderName+" header value", "received-accept", resp.Header.Get(ProtoMagicAcceptHeaderName), "expected", expected)

				return errors.Join(
					ErrInvalidWorkforceAccept,
					retry.ErrDoNotRetry,
					rungroup.ErrShutdownAll,
				)
			}
			return nil
		})
	}
)

func GenerateWorkforceKey() string {
	key := make([]byte, 16)

	if _, err := rand.Read(key); err != nil {
		return ""
	}

	return base64.StdEncoding.EncodeToString(key)
}

func GenerateWorkforceAccept(key string) string {
	hash := sha1.Sum([]byte(key + ProtoUpgradeMagic))

	return base64.StdEncoding.EncodeToString(hash[:])
}

func ValidateWorkforceAccept(key, accept string) bool {
	expected := GenerateWorkforceAccept(key)

	// Constant-time comparison
	return subtle.ConstantTimeCompare(
		[]byte(expected),
		[]byte(accept),
	) == 1
}
