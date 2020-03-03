// +build windows

package mapping

import (
	"golang.org/x/sys/windows"
	"os"
	"reflect"
	"unsafe"
)

// Mapping of a file
type Mapping struct {
	handle windows.Handle
	length int
	addr   uintptr
}

// Bytes return mapped memory
func (m *Mapping) Bytes() (b []byte) {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = m.addr
	h.Cap = m.length
	h.Len = h.Cap
	return
}

// Create a mapping from a file
func Create(file *os.File) (m *Mapping, err error) {
	info, err := file.Stat()
	if err != nil {
		return
	}
	size := info.Size()
	handle, err := windows.CreateFileMapping(windows.Handle(file.Fd()), nil, windows.PAGE_READWRITE|0x8000000, 0, uint32(size), nil)
	if err != nil {
		panic(err)
		return
	}
	addr, err := windows.MapViewOfFile(handle, windows.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		_ = windows.CloseHandle(handle)
		return
	}
	m = &Mapping{
		handle: handle,
		length: int(size),
		addr:   addr,
	}
	return
}

// Close a mapping
func (m *Mapping) Close() (err error) {
	err = windows.UnmapViewOfFile(m.addr)
	if e := windows.CloseHandle(m.handle); err == nil {
		err = e
	}
	return
}
