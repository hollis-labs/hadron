package tetherhost

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode/utf8"

	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
)

// ResultMailbox is the address every result reply must be sent to.
const ResultMailbox = "msg://agent/hadron/results"

// ReplyKindResponse is the only reply kind accepted as a result.
const ReplyKindResponse = "response"

// SecretFileName is the per-Hadron result secret under the data directory.
const SecretFileName = "agent-result-secret.key"

const (
	secretSize = 32
	// maxTetherKeyBytes is Tether's idempotency key limit.
	maxTetherKeyBytes = 512
	tetherKeyPrefix   = "hadron/"
	// nonceDomain versions the nonce derivation.
	nonceDomain = "hadron-agent-result-nonce/v1"
)

// TetherKey maps a launch to its Tether idempotency key:
//
//	hadron/<run>/<node>/<iteration>/<k>
//
// k is the first 16 hex characters of sha256(IdempotencyKey) and an empty
// iteration is "-". The key stays within 512 bytes: when it would not, the
// run, node and iteration components are shortened (longest first, at rune
// boundaries). k is never shortened, so uniqueness rests on it.
func TetherKey(request agentadapter.LaunchRequest) string {
	sum := sha256.Sum256([]byte(request.IdempotencyKey))
	k := hex.EncodeToString(sum[:])[:16]
	iteration := request.Identity.Iteration
	if iteration == "" {
		iteration = "-"
	}
	parts := []string{request.Identity.RunID, request.Identity.NodeID, iteration}
	budget := maxTetherKeyBytes - len(tetherKeyPrefix) - len(parts) - len(k)
	parts = fitComponents(parts, budget)
	return tetherKeyPrefix + strings.Join(parts, "/") + "/" + k
}

// fitComponents shortens the longest components until their byte total fits
// budget, giving every component an equal share of what it cannot use.
func fitComponents(parts []string, budget int) []string {
	total := 0
	for _, part := range parts {
		total += len(part)
	}
	if total <= budget {
		return parts
	}
	// Water-fill: find the largest cap such that sum(min(len, cap)) <= budget.
	low, high := 0, budget
	for low < high {
		middle := (low + high + 1) / 2
		used := 0
		for _, part := range parts {
			used += min(len(part), middle)
		}
		if used <= budget {
			low = middle
		} else {
			high = middle - 1
		}
	}
	result := make([]string, len(parts))
	for index, part := range parts {
		result[index] = truncateRunes(part, low)
	}
	return result
}

func truncateRunes(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

// ResultNonce derives the nonce an agent must echo in its result reply:
//
//	hex(HMAC-SHA256(secret, "hadron-agent-result-nonce/v1|" + requestDigest + "|" + correlation))
//
// requestDigest is LaunchRequest.Digest() and correlation is
// LaunchRequest.Correlation. Both are carried by the durable SessionRef
// (RequestDigest, Correlation), so observation after a restart recomputes the
// nonce from the ref alone, while the digest (which covers the idempotency
// key, identity, prompt and inputs) keeps it unique per launch.
func ResultNonce(secret []byte, requestDigest, correlation string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = io.WriteString(mac, nonceDomain+"|"+requestDigest+"|"+correlation)
	return hex.EncodeToString(mac.Sum(nil))
}

// PromptAppend renders the text appended to the Tether launch prompt: the
// step's prompt, if any, then the delimited result contract. It is a pure
// function of its arguments, which Tether's idempotency digest relies on.
func PromptAppend(prompt, correlation, nonce string) string {
	var builder strings.Builder
	if prompt != "" {
		builder.WriteString(prompt)
		builder.WriteString("\n\n")
	}
	builder.WriteString("----- BEGIN HADRON RESULT CONTRACT -----\n")
	builder.WriteString("When your task is complete, send exactly one message with the mux_message_send tool:\n")
	builder.WriteString("  kind: " + ReplyKindResponse + "\n")
	builder.WriteString("  to: " + ResultMailbox + "\n")
	builder.WriteString("  thread_id: " + correlation + "\n")
	builder.WriteString(`  payload: {"nonce": "` + nonce + `", "result": <your result as a JSON value>}` + "\n")
	builder.WriteString("Do not send the nonce anywhere else. Hadron treats the first valid reply on this thread as your step result.\n")
	builder.WriteString("----- END HADRON RESULT CONTRACT -----\n")
	return builder.String()
}

// errInvalidReply explains why a reply was not accepted as a result.
type errInvalidReply string

func (e errInvalidReply) Error() string { return string(e) }

// parseResultReply returns the reply's raw "result" JSON when the reply is a
// valid result for wantNonce. The nonce is compared in constant time. Reply
// From is deliberately not checked: Tether's from is caller-asserted.
func parseResultReply(reply Reply, wantNonce string) (json.RawMessage, error) {
	if reply.Kind != ReplyKindResponse {
		return nil, errInvalidReply("reply kind is not " + ReplyKindResponse)
	}
	if reply.To != ResultMailbox {
		return nil, errInvalidReply("reply is not addressed to " + ResultMailbox)
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.Payload))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return nil, errInvalidReply("reply payload is not a JSON object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errInvalidReply("reply payload has trailing data")
	}
	var nonce string
	if raw, ok := fields["nonce"]; !ok || json.Unmarshal(raw, &nonce) != nil {
		return nil, errInvalidReply("reply payload nonce is missing or not a string")
	}
	if !hmac.Equal([]byte(nonce), []byte(wantNonce)) {
		return nil, errInvalidReply("reply nonce does not match")
	}
	result, ok := fields["result"]
	if !ok {
		return nil, errInvalidReply("reply payload has no result")
	}
	return result, nil
}

// LoadOrCreateSecret returns the 32-byte per-Hadron result secret stored at
// <dataDir>/agent-result-secret.key, creating it (mode 0600) on first use.
// Concurrent first opens publish exactly one secret: each candidate is
// written to a private temporary file and hard-linked into place, and a loser
// reads the winner. An unsafe existing file (not regular, a symlink, readable
// by group or others, or the wrong size) is refused rather than repaired.
func LoadOrCreateSecret(dataDir string) ([]byte, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("agent result secret requires a data directory")
	}
	path := filepath.Join(dataDir, SecretFileName)
	secret, err := readSecret(path)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return secret, err
	}
	candidate := make([]byte, secretSize)
	if _, randomErr := rand.Read(candidate); randomErr != nil {
		return nil, randomErr
	}
	temp, err := os.CreateTemp(dataDir, SecretFileName+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create agent result secret: %w", err)
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	_, writeErr := temp.Write(candidate)
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if joined := errors.Join(writeErr, syncErr, closeErr); joined != nil {
		return nil, fmt.Errorf("write agent result secret: %w", joined)
	}
	if linkErr := os.Link(tempPath, path); linkErr != nil && !errors.Is(linkErr, os.ErrExist) {
		return nil, fmt.Errorf("publish agent result secret: %w", linkErr)
	}
	if directory, openErr := os.Open(filepath.Clean(dataDir)); openErr == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return readSecret(path)
}

func readSecret(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // #nosec G304 -- path is the host-owned data directory secret.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("open agent result secret: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() != secretSize {
		return nil, errors.New("agent result secret file is unsafe: want a 32-byte regular file with mode 0600")
	}
	secret := make([]byte, secretSize)
	if _, err := io.ReadFull(file, secret); err != nil {
		return nil, fmt.Errorf("read agent result secret: %w", err)
	}
	return secret, nil
}
