//go:build !unix

package unattended

import "io/fs"

// fileOwner cannot establish ownership off unix, so the file is refused.
func fileOwner(fs.FileInfo) (int, bool) { return 0, false }
