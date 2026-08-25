// Package generatedapi turns bounded external API descriptions into ordinary
// workflow step kinds. Generated kinds expose exact schemas, effects,
// capabilities, and credential-reference inputs before execution, then route
// the actual request through the existing HTTP adapter.
//
// The initial source family is a deliberately portable OpenAPI 3.0/3.1
// subset. Unsupported or ambiguous source features fail generation instead of
// being ignored or implemented by a second runtime. HTTP Basic sources require
// x-hadron-basic-username as a fixed non-secret username; the generated input
// remains a SecretRef-only password and is resolved by http@v1. Custom apiKey
// schemes must use a dedicated non-cookie, non-Authorization request header;
// Authorization credentials use the explicit bearer or Basic mappings.
package generatedapi
