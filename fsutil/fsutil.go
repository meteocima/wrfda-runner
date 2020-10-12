package fsutil

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sync"

	"github.com/hpcloud/tail"
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

// Logf ...
func Logf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}

// Copy ...
func (tr *Transaction) Copy(from, to Path) {
	if tr.Err != nil {
		return
	}

	Logf("\tCopy from %s to %s\n", from, to)
	source, err := os.Open(tr.Root.JoinP(from).String())
	if err != nil {
		tr.Err = err
		return
	}
	defer source.Close()

	target, err := os.OpenFile(tr.Root.JoinP(to).String(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0664))
	if err != nil {
		tr.Err = err
		return
	}
	defer target.Close()

	_, tr.Err = io.Copy(target, source)

}

// Save ...
func (tr *Transaction) Save(targetPath Path, content []byte) {
	if tr.Err != nil {
		return
	}

	tr.Err = ioutil.WriteFile(
		tr.Root.JoinP(targetPath).String(),
		content,
		os.FileMode(0664),
	)
}

// Link ...
func (tr *Transaction) Link(from, to Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tLink from %s to %s\n", from, to)
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
	Logf("\tRmDir %s\n", dir)

	tr.Err = os.RemoveAll(tr.Root.JoinP(dir).String())
}

// RmFile ...
func (tr *Transaction) RmFile(file Path) {
	if tr.Err != nil {
		return
	}
	Logf("\tRmFile %s\n", file)
	tr.Err = os.Remove(tr.Root.JoinP(file).String())
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
		tr.Err = os.Remove(tr.Root.JoinP(logFile).String())
		if os.IsNotExist(tr.Err) {
			tr.Err = nil
		}
		if tr.Err != nil {
			return
		}
	}

	output, pwrite := io.Pipe()

	var tailProc *tail.Tail
	if logFile == "" {
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			tr.Err = err
			return
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			tr.Err = err
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
			tr.Err = err
			return
		}
		tailProc = tail

		go func() {
			for l := range tail.Lines {
				pwrite.Write([]byte(l.Text + "\n"))
				if l.Err != nil {
					tr.Err = l.Err
					break
				}
			}
			pwrite.Close()
		}()

	}

	tr.Err = cmd.Start()
	if tr.Err != nil {
		return
	}

	go func() {
		stdoutBuff := bufio.NewReader(output)
		line, _, err := stdoutBuff.ReadLine()
		for line != nil {
			line, _, err = stdoutBuff.ReadLine()
			if err != nil && err != io.EOF {
				panic(err)
			}
			fmt.Println(string(line))
		}
	}()
	tr.Err = cmd.Wait()
	if tailProc != nil {
		tailProc.Stop()
	}
}
