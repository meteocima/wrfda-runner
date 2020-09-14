package fsutil

import (
	"fmt"
	"os"
	"os/exec"
	"path"
)

// Path ...
type Path string

// Transaction ...
type Transaction struct {
	Root Path
	Err  error
}

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

// Exists ...
func (tr *Transaction) Exists(file Path) bool {
	if tr.Err != nil {
		return false
	}
	_, err := os.Stat(tr.Root.JoinP(file).String())
	if !os.IsNotExist(err) {
		tr.Err = err
	}
	return err == nil
}

// Copy ...
func (tr *Transaction) Copy(from, to Path) {}

// Link ...
func (tr *Transaction) Link(from, to Path) {
	if tr.Err != nil {
		return
	}
	tr.Err = os.Symlink(
		tr.Root.JoinP(from).String(),
		tr.Root.JoinP(to).String(),
	)
}

// MkDir ...
func (tr *Transaction) MkDir(dir Path) {
	if tr.Err != nil {
		return
	}
	tr.Err = os.MkdirAll(tr.Root.JoinP(dir).String(), os.FileMode(0755))
}

// RmDir ...
func (tr *Transaction) RmDir(dir Path) {
	if tr.Err != nil {
		return
	}
	tr.Err = os.RemoveAll(tr.Root.JoinP(dir).String())
}

// RmFile ...
func (tr *Transaction) RmFile(file Path) {
	if tr.Err != nil {
		return
	}
	tr.Err = os.Remove(tr.Root.JoinP(file).String())
}

// Run ...
func (tr *Transaction) Run(cwd Path, command string) {
	if tr.Err != nil {
		return
	}
	os.Chdir(tr.Root.JoinP(cwd).String())
	cmd := exec.Command(command)
	tr.Err = cmd.Run()
}
