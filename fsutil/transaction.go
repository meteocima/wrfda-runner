package fsutil

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/hpcloud/tail"
)

// Transaction ...
type Transaction struct {
	Root Path
	Err  error
}

// Exists ...
func (tr *Transaction) Exists(file Path) bool {
	if tr.Err != nil {
		return false
	}
	_, err := os.Stat(tr.Root.JoinP(file).String())
	if !os.IsNotExist(err) && err != nil {
		tr.Err = fmt.Errorf("Exists `%s`: Stat error: %w", file.String(), err)
	}
	return err == nil
}

// ReaddirAbs ...
func (tr *Transaction) ReaddirAbs(dir Path) []string {
	if tr.Err != nil {
		return nil
	}
	dirfd, err := os.Open(dir.String())
	if err != nil {
		tr.Err = fmt.Errorf("ReaddirAbs `%s`: Open error: %w", dir.String(), err)
		return nil
	}

	defer dirfd.Close()

	res, err := dirfd.Readdirnames(0)
	if !os.IsNotExist(err) {
		tr.Err = fmt.Errorf("ReaddirAbs `%s`: Readdirnames error: %w", dir.String(), err)
	}
	return res
}

// Readdir ...
func (tr *Transaction) Readdir(dir Path) []string {
	return tr.ReaddirAbs(tr.Root.JoinP(dir))
}

// Logf ...
func Logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

// Copy ...
func (tr *Transaction) Copy(from, to Path) {
	tr.CopyAbs(tr.Root.JoinP(from), to)
}

// CopyAbs ...
func (tr *Transaction) CopyAbs(from, to Path) {
	if tr.Err != nil {
		return
	}

	Logf("\tCopy from %s to %s\n", from, to)
	source, err := os.Open(from.String())
	if err != nil {
		tr.Err = fmt.Errorf("CopyAbs from `%s` to `%s`: Open error: %w", from.String(), to.String(), err)
		return
	}
	defer source.Close()

	target, err := os.OpenFile(tr.Root.JoinP(to).String(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0664))
	if err != nil {
		tr.Err = fmt.Errorf("CopyAbs from `%s` to `%s`: OpenFile error: %w", from.String(), to.String(), err)
		return
	}
	defer target.Close()

	_, err = io.Copy(target, source)
	if err != nil {
		tr.Err = fmt.Errorf("CopyAbs from `%s` to `%s`: Copy error: %w", from.String(), to.String(), err)
	}

}

// Save ...
func (tr *Transaction) Save(targetPath Path, content []byte) {
	if tr.Err != nil {
		return
	}

	err := ioutil.WriteFile(
		tr.Root.JoinP(targetPath).String(),
		content,
		os.FileMode(0664),
	)
	if err != nil {
		tr.Err = fmt.Errorf("Save to `%s`: WriteFile error: %w", targetPath.String(), err)
	}
}

// Link ...
func (tr *Transaction) Link(from, to Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tLink from %s to %s (root %s)\n", tr.Root.JoinP(from).String(), tr.Root.JoinP(to).String(), tr.Root.String())
	err := os.Symlink(
		tr.Root.JoinP(from).String(),
		tr.Root.JoinP(to).String(),
	)
	if err != nil {
		tr.Err = fmt.Errorf("Link from `%s` to `%s`: Symlink error: %w", from.String(), to.String(), err)
	}
}

// LinkAbs ...
func (tr *Transaction) LinkAbs(from, to Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tLink from %s to %s\n", from, to)
	err := os.Symlink(
		from.String(),
		tr.Root.JoinP(to).String(),
	)
	if err != nil {
		tr.Err = fmt.Errorf("LinkAbs from `%s` to `%s`: Symlink error: %w", from.String(), to.String(), err)
	}
}

// MkDir ...
func (tr *Transaction) MkDir(dir Path) {
	if tr.Err != nil {
		return
	}

	err := os.MkdirAll(tr.Root.JoinP(dir).String(), os.FileMode(0755))
	if err != nil {
		tr.Err = fmt.Errorf("MkDir `%s`: MkdirAll error: %w", dir.String(), err)
	}
}

// RmDir ...
func (tr *Transaction) RmDir(dir Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tRmDir %s\n", dir)

	err := os.RemoveAll(tr.Root.JoinP(dir).String())
	if err != nil {
		tr.Err = fmt.Errorf("RmDir `%s`: RemoveAll error: %w", dir.String(), err)
	}
}

// RmFile ...
func (tr *Transaction) RmFile(file Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tRmFile %s\n", file)
	err := os.Remove(tr.Root.JoinP(file).String())
	if err != nil {
		tr.Err = fmt.Errorf("RmFile `%s`: Remove error: %w", file.String(), err)
	}
}

// Run ...
func (tr *Transaction) Run(cwd Path, logFile Path, command string, args ...string) {
	if tr.Err != nil {
		return
	}
	Logf("\tRun %s %s\n", command, args)
	cmd := exec.Command(command, args...)
	cmd.Dir = tr.Root.JoinP(cwd).String()

	if logFile != "" {
		err := os.Remove(tr.Root.JoinP(logFile).String())
		if err != nil && !os.IsNotExist(err) {
			tr.Err = fmt.Errorf("Run `%s`: Remove error: %w", command, err)
			return
		}
	}

	output, pwrite := io.Pipe()

	var tailProc *tail.Tail
	if logFile == "" {
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			tr.Err = fmt.Errorf("Run `%s`: StdoutPipe error: %w", command, err)
			return
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			tr.Err = fmt.Errorf("Run `%s`: StderrPipe error: %w", command, err)
			return
		}

		go func() {
			done := sync.WaitGroup{}
			done.Add(2)
			go func() {
				io.Copy(pwrite, stdout)
				done.Done()
			}()

			go func() {
				io.Copy(pwrite, stderr)
				done.Done()
			}()

			done.Wait()

			pwrite.Close()
		}()

	} else {

		tail, err := tail.TailFile(tr.Root.JoinP(logFile).String(), tail.Config{
			Follow:    true,
			MustExist: false,
			ReOpen:    true,
		})

		if err != nil {
			tr.Err = fmt.Errorf("Run `%s`: TailFile error: %w", command, err)
			return
		}
		tailProc = tail

		go func() {
			for l := range tail.Lines {
				pwrite.Write([]byte(l.Text + "\n"))
				if l.Err != nil {
					tr.Err = fmt.Errorf("Run `%s`: TailFile error (lines): %w", command, err)
					break
				}
			}
			pwrite.Close()
		}()

	}

	err := cmd.Start()
	if tr.Err != nil {
		tr.Err = fmt.Errorf("Run `%s`: Start error: %w", command, err)
		return
	}

	go func() {
		stdoutBuff := bufio.NewReader(output)
		line, _, err := stdoutBuff.ReadLine()
		for line != nil {
			line, _, err = stdoutBuff.ReadLine()
			if err != nil && err != io.EOF {
				tr.Err = fmt.Errorf("Run `%s`: ReadLine error: %w", command, err)
			}
			fmt.Println(string(line))
		}
	}()
	err = cmd.Wait()

	if err != nil {
		tr.Err = fmt.Errorf("Run `%s`: Wait error: %w", command, err)
	}

	if tailProc != nil {
		tailProc.Stop()
	}
}
