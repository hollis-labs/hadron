package persistence

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
)

type storedCredential struct {
	metadata hoststate.CredentialMetadata
	digest   string
}

func credentialTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	v, err := parseWorkflowTime("credential time", value.String)
	if err != nil {
		return nil, errors.New("stored credential time is invalid")
	}
	v = v.UTC()
	return &v, nil
}

func scanCredential(row workflowScanner, now time.Time) (storedCredential, error) {
	var out storedCredential
	var created string
	var expires, overlap, last, revoked sql.NullString
	err := row.Scan(&out.metadata.CredentialID, &out.metadata.PrincipalID, &out.digest, &created, &expires, &overlap, &last, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return out, workflowruntime.ErrNotFound
	}
	if err != nil {
		return out, errors.New("credential storage unavailable")
	}
	if !hoststate.ValidCredentialID(out.metadata.CredentialID) {
		return out, errors.New("stored credential is invalid")
	}
	if out.metadata.CreatedAt, err = parseWorkflowTime("credential created", created); err != nil {
		return out, errors.New("stored credential time is invalid")
	}
	if out.metadata.ExpiresAt, err = credentialTime(expires); err != nil {
		return out, err
	}
	if out.metadata.OverlapUntil, err = credentialTime(overlap); err != nil {
		return out, err
	}
	if out.metadata.LastUsedAt, err = credentialTime(last); err != nil {
		return out, err
	}
	if out.metadata.RevokedAt, err = credentialTime(revoked); err != nil {
		return out, err
	}
	out.metadata.Status = "active"
	if out.metadata.RevokedAt != nil {
		out.metadata.Status = "revoked"
	} else if out.metadata.ExpiresAt != nil && !now.Before(*out.metadata.ExpiresAt) {
		out.metadata.Status = "expired"
	}
	return out, nil
}

const credentialColumns = `credential_id, principal_id, credential_digest, created_at, expires_at, overlap_until, last_used_at, revoked_at`

func loadCredential(ctx context.Context, q workflowSQL, principal, id string, now time.Time) (storedCredential, error) {
	return scanCredential(q.QueryRowContext(ctx, `SELECT `+credentialColumns+` FROM workflow_mcp_credentials WHERE principal_id=? AND credential_id=?`, principal, id), now)
}

func listCredentials(ctx context.Context, q workflowSQL, principal string, now time.Time) ([]storedCredential, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+credentialColumns+` FROM workflow_mcp_credentials WHERE principal_id=? ORDER BY created_at, credential_id`, principal)
	if err != nil {
		return nil, errors.New("credential storage unavailable")
	}
	defer closeRows(rows)
	result := make([]storedCredential, 0)
	for rows.Next() {
		record, err := scanCredential(rows, now)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if rows.Err() != nil {
		return nil, errors.New("credential storage unavailable")
	}
	return result, nil
}

func (s *WorkflowExposureStore) ListMCPCredentials(ctx context.Context, principal string, admin hoststate.CredentialAdministrator) (hoststate.CredentialList, error) {
	var result hoststate.CredentialList
	err := s.state.write(ctx, "inspect credentials", func(q workflowSQL) error {
		if err := admin.Validate(ctx); err != nil {
			return err
		}
		p, err := loadMCPPrincipal(ctx, q, "principal_id = ?", principal)
		if err != nil {
			return err
		}
		credentials, err := listCredentials(ctx, q, principal, s.now().UTC())
		if err != nil {
			return err
		}
		result = hoststate.CredentialList{PrincipalID: principal, Generation: p.Generation, Credentials: make([]hoststate.CredentialMetadata, 0, len(credentials))}
		for _, c := range credentials {
			c.metadata.Generation = p.Generation
			result.Credentials = append(result.Credentials, c.metadata)
		}
		return nil
	})
	return result, err
}

func credentialRequestDigest(request any) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", errors.New("invalid credential request")
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func credentialKeyDigest(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}

func replayCredentialOperation(ctx context.Context, q workflowSQL, principal, key, actor, operation, digest string, now time.Time) (hoststate.CredentialMetadata, bool, error) {
	var savedActor, savedOperation, savedDigest, id string
	var generation uint64
	err := q.QueryRowContext(ctx, `SELECT actor,operation,request_digest,credential_id,operation_generation FROM workflow_credential_operations WHERE principal_id=? AND key_digest=?`, principal, credentialKeyDigest(key)).Scan(&savedActor, &savedOperation, &savedDigest, &id, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.CredentialMetadata{}, false, nil
	}
	if err != nil {
		return hoststate.CredentialMetadata{}, false, errors.New("credential receipt unavailable")
	}
	if savedActor != actor || savedOperation != operation || savedDigest != digest {
		return hoststate.CredentialMetadata{}, false, fmt.Errorf("%w: credential request conflicts", hoststate.ErrConflict)
	}
	p, err := loadMCPPrincipal(ctx, q, "principal_id = ?", principal)
	if err != nil {
		return hoststate.CredentialMetadata{}, false, err
	}
	c, err := loadCredential(ctx, q, principal, id, now)
	if err != nil {
		return hoststate.CredentialMetadata{}, false, err
	}
	c.metadata.Generation, c.metadata.OperationGeneration, c.metadata.Replayed = p.Generation, generation, true
	return c.metadata, true, nil
}

func writeCredentialOperation(ctx context.Context, q workflowSQL, admin hoststate.CredentialAdministrator, principal, id, key, operation, digest string, generation uint64, now time.Time) error {
	_, err := q.ExecContext(ctx, `INSERT INTO workflow_credential_operations(principal_id,key_digest,operation,actor,request_digest,credential_id,operation_generation,created_at) VALUES(?,?,?,?,?,?,?,?)`, principal, credentialKeyDigest(key), operation, admin.Actor, digest, id, generation, workflowTime(now))
	if err != nil {
		return errors.New("credential receipt could not be persisted")
	}
	_, err = q.ExecContext(ctx, `INSERT INTO workflow_credential_audit(principal_id,credential_id,generation,operation,actor,source,created_at) VALUES(?,?,?,?,?,?,?)`, principal, id, generation, operation, admin.Actor, admin.Source, workflowTime(now))
	if err != nil {
		return errors.New("credential audit could not be persisted")
	}
	return nil
}

func credentialGeneration(ctx context.Context, q workflowSQL, principal string, expected uint64, now time.Time) (uint64, error) {
	p, err := loadMCPPrincipal(ctx, q, "principal_id = ?", principal)
	if err != nil {
		return 0, err
	}
	if p.Generation != expected {
		return 0, exposureCAS("credential family", expected, p.Generation)
	}
	if p.Generation >= math.MaxInt64 {
		return 0, fmt.Errorf("%w: credential generation exhausted", hoststate.ErrConflict)
	}
	result, err := q.ExecContext(ctx, `UPDATE workflow_mcp_principals SET generation=generation+1, updated_at=? WHERE principal_id=? AND generation=?`, workflowTime(now), principal, expected)
	if err != nil {
		return 0, errors.New("credential generation update failed")
	}
	if err := expectExposureRow(result, "credential family", expected); err != nil {
		return 0, err
	}
	return expected + 1, nil
}

func newCredentialID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", errors.New("credential randomness unavailable")
	}
	return "cred_" + hex.EncodeToString(id[:]), nil
}

func insertInitialCredential(ctx context.Context, q workflowSQL, principal, digest string, now time.Time) error {
	id, err := newCredentialID()
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO workflow_mcp_credentials(credential_id,principal_id,credential_digest,created_at) VALUES(?,?,?,?)`, id, principal, digest, workflowTime(now))
	if err != nil {
		return errors.New("initial credential could not be persisted")
	}
	return nil
}

func (s *WorkflowExposureStore) IssueMCPCredential(ctx context.Context, request hoststate.IssueCredentialRequest, admin hoststate.CredentialAdministrator) (hoststate.CredentialIssue, error) {
	var result hoststate.CredentialIssue
	if err := request.Validate(); err != nil {
		return result, fmt.Errorf("%w: invalid credential issuance", hoststate.ErrInvalidRecord)
	}
	// The effective default is normalized before hashing, so equivalent clients
	// have one retry contract; submitted generation is deliberately retained.
	normalized := request
	overlap := request.EffectiveOverlapSeconds()
	normalized.OverlapSeconds = &overlap
	digest, err := credentialRequestDigest(normalized)
	if err != nil {
		return result, err
	}
	err = s.state.write(ctx, "issue credential", func(q workflowSQL) error {
		if err := admin.Validate(ctx); err != nil {
			return err
		}
		now := s.now().UTC()
		metadata, replayed, err := replayCredentialOperation(ctx, q, request.PrincipalID, request.IdempotencyKey, admin.Actor, "issue", digest, now)
		if err != nil {
			return err
		}
		if replayed {
			result.CredentialMetadata = metadata
			return nil
		}
		if _, err := loadCredential(ctx, q, request.PrincipalID, request.CredentialID, now); err != nil {
			return err
		}
		generation, err := credentialGeneration(ctx, q, request.PrincipalID, request.ExpectedGeneration, now)
		if err != nil {
			return err
		}
		prior, err := listCredentials(ctx, q, request.PrincipalID, now)
		if err != nil {
			return err
		}
		deadline := now.Add(time.Duration(overlap) * time.Second)
		for _, c := range prior {
			if c.metadata.Status != "active" {
				continue
			}
			expires := deadline
			if c.metadata.ExpiresAt != nil && c.metadata.ExpiresAt.Before(expires) {
				expires = *c.metadata.ExpiresAt
			}
			cap := deadline
			if c.metadata.OverlapUntil != nil && c.metadata.OverlapUntil.Before(cap) {
				cap = *c.metadata.OverlapUntil
			}
			if _, err := q.ExecContext(ctx, `UPDATE workflow_mcp_credentials SET expires_at=?,overlap_until=? WHERE credential_id=? AND principal_id=?`, workflowTime(expires), workflowTime(cap), c.metadata.CredentialID, request.PrincipalID); err != nil {
				return errors.New("credential overlap update failed")
			}
		}
		var secret [32]byte
		if _, err := rand.Read(secret[:]); err != nil {
			return errors.New("credential randomness unavailable")
		}
		value := "hmc_" + base64.RawURLEncoding.EncodeToString(secret[:])
		verifier, err := hoststate.DigestMCPToken(value)
		if err != nil {
			return errors.New("generated credential invalid")
		}
		id, err := newCredentialID()
		if err != nil {
			return err
		}
		var expiry any
		if request.TTLSeconds != nil {
			expiry = workflowTime(now.Add(time.Duration(*request.TTLSeconds) * time.Second))
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO workflow_mcp_credentials(credential_id,principal_id,credential_digest,created_at,expires_at) VALUES(?,?,?,?,?)`, id, request.PrincipalID, verifier, workflowTime(now), expiry); err != nil {
			return errors.New("credential could not be persisted")
		}
		if err := writeCredentialOperation(ctx, q, admin, request.PrincipalID, id, request.IdempotencyKey, "issue", digest, generation, now); err != nil {
			return err
		}
		c, err := loadCredential(ctx, q, request.PrincipalID, id, now)
		if err != nil {
			return err
		}
		c.metadata.Generation, c.metadata.OperationGeneration, c.metadata.SecretAvailable = generation, generation, true
		result = hoststate.CredentialIssue{CredentialMetadata: c.metadata, Secret: value}
		return nil
	})
	if err != nil {
		return hoststate.CredentialIssue{}, err
	}
	return result, nil
}

func (s *WorkflowExposureStore) RevokeMCPCredential(ctx context.Context, request hoststate.RevokeCredentialRequest, admin hoststate.CredentialAdministrator) (hoststate.CredentialMetadata, error) {
	var result hoststate.CredentialMetadata
	if err := request.Validate(); err != nil {
		return result, fmt.Errorf("%w: invalid credential revocation", hoststate.ErrInvalidRecord)
	}
	digest, err := credentialRequestDigest(request)
	if err != nil {
		return result, err
	}
	err = s.state.write(ctx, "revoke credential", func(q workflowSQL) error {
		if err := admin.Validate(ctx); err != nil {
			return err
		}
		now := s.now().UTC()
		metadata, replayed, err := replayCredentialOperation(ctx, q, request.PrincipalID, request.IdempotencyKey, admin.Actor, "revoke", digest, now)
		if err != nil {
			return err
		}
		if replayed {
			result = metadata
			return nil
		}
		c, err := loadCredential(ctx, q, request.PrincipalID, request.CredentialID, now)
		if err != nil {
			return err
		}
		if c.metadata.RevokedAt != nil {
			return fmt.Errorf("%w: credential is already revoked", hoststate.ErrConflict)
		}
		generation, err := credentialGeneration(ctx, q, request.PrincipalID, request.ExpectedGeneration, now)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `UPDATE workflow_mcp_credentials SET revoked_at=? WHERE credential_id=? AND principal_id=?`, workflowTime(now), request.CredentialID, request.PrincipalID); err != nil {
			return errors.New("credential revocation failed")
		}
		if err := writeCredentialOperation(ctx, q, admin, request.PrincipalID, request.CredentialID, request.IdempotencyKey, "revoke", digest, generation, now); err != nil {
			return err
		}
		c, err = loadCredential(ctx, q, request.PrincipalID, request.CredentialID, now)
		if err != nil {
			return err
		}
		c.metadata.Generation, c.metadata.OperationGeneration = generation, generation
		result = c.metadata
		return nil
	})
	return result, err
}

// resolveCredential validates and records actual successful credential use in
// one transaction. Observation never changes the family generation.
func (s *WorkflowExposureStore) resolveCredential(ctx context.Context, digest string) (hoststate.MCPPrincipalSnapshot, error) {
	var result hoststate.MCPPrincipalSnapshot
	err := s.state.write(ctx, "authenticate credential", func(q workflowSQL) error {
		now := s.now().UTC()
		c, err := scanCredential(q.QueryRowContext(ctx, `SELECT `+credentialColumns+` FROM workflow_mcp_credentials WHERE credential_digest=?`, digest), now)
		if err != nil {
			return err
		}
		if c.metadata.Status != "active" || !hoststate.MatchMCPTokenDigest(c.digest, digest) {
			return workflowruntime.ErrNotFound
		}
		p, err := loadMCPPrincipal(ctx, q, "principal_id = ?", c.metadata.PrincipalID)
		if err != nil {
			return err
		}
		if c.metadata.LastUsedAt == nil || c.metadata.LastUsedAt.Before(now) {
			if _, err := q.ExecContext(ctx, `UPDATE workflow_mcp_credentials SET last_used_at=? WHERE credential_id=?`, workflowTime(now), c.metadata.CredentialID); err != nil {
				return errors.New("credential observation could not be persisted")
			}
		}
		result = p
		return nil
	})
	return result, err
}
