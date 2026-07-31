// Package migrations embeds the immutable Goose migration set into the runtime binary.
package migrations

import "embed"

// Files contains every versioned SQL migration shipped with this build.
//
//go:embed *.sql
var Files embed.FS
