// Package gateadapter implements human_gate@v1 over the shared workflow/gate
// vocabulary and canonical workflow/wait continuation.
//
// The adapter persists no application records and performs no polling.
// Product presentation and responder policy are supplied through injected
// extraction-ready seams. Payload stores must be idempotent and own cleanup
// if payload storage succeeds but durable suspension later fails.
package gateadapter
