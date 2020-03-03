// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package mapping

import (
	"golang.org/x/sys/unix"
	"os"
)

// Mapping of a file
type Mapping struct {
	data []byte
}

// Bytes return mapped memory
func (m *Mapping) Bytes() (b []byte) {
	return m.data
}

// Create a mapping from a file
func Create(file *os.File) (m *Mapping, err error) {
	info, err := file.Stat()
	if err != nil {
		return
	}
	size := info.Size()
	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return
	}
	m = &Mapping{
		data: data,
	}
	return
}

// Close a mapping
func (m *Mapping) Close() (err error) {
	return unix.Munmap(m.data)
}
