package fsutil

import (
	"fmt"
	"path"
)

// Path ...
type Path string

// Join ...
func (pt Path) Join(part string) Path {
	return Path(path.Join(string(pt), part))
}

// JoinP ...
func (pt Path) JoinP(part Path) Path {
	return Path(path.Join(string(pt), string(part)))
}

// JoinF ...
func (pt Path) JoinF(part string, args ...interface{}) Path {
	partF := fmt.Sprintf(part, args...)
	return Path(path.Join(string(pt), partF))
}

// PathF ...
func PathF(format string, args ...interface{}) Path {
	p := fmt.Sprintf(format, args...)
	return Path(p)
}

func (pt Path) String() string {
	return string(pt)
}
