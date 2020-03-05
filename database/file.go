package database

import (
	"errors"
	"github.com/fengyoulin/shm/mapping"
	"os"
	"time"
)

// ErrTimeout when waiting for database init
var ErrTimeout = errors.New("timeout when waiting for database init")

// Open a database file, return a mapping
func Open(path string, size int, wait time.Duration) (m *mapping.Mapping, unlock func() error, err error) {
	var lock *os.File
	name := path + ".lock"
	for i := 0; i < int(wait/time.Millisecond/10); i++ {
		lock, err = os.OpenFile(name, os.O_CREATE|os.O_EXCL, 0664)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		err = ErrTimeout
		return
	}
	uf := func() (er error) {
		er = lock.Close()
		if e := os.Remove(name); er == nil {
			er = e
		}
		return
	}
	defer func() {
		if uf == nil {
			return
		}
		if e := uf(); err == nil {
			err = e
		}
	}()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return
	}
	defer func() {
		if e := f.Close(); err == nil {
			err = e
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return
	}
	// created new file
	if info.Size() == 0 {
		var buf [4096]byte
		for i := 0; i < size/4096; i++ {
			_, err = f.Write(buf[:])
			if err != nil {
				return
			}
		}
		if s := size % 4096; s > 0 {
			_, err = f.Write(buf[:s])
			if err != nil {
				return
			}
		}
	}
	m, err = mapping.Create(f)
	if err != nil {
		return
	}
	unlock, uf = uf, nil
	return
}
