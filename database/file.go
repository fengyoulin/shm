package database

import (
	"github.com/fengyoulin/shm/mapping"
	"os"
)

// Open a database file, return a mapping
func Open(path string, size int) (m *mapping.Mapping, err error) {
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
	return mapping.Create(f)
}
