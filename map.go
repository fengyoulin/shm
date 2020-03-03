package shm

import (
	"errors"
	"github.com/fengyoulin/shm/database"
	"github.com/fengyoulin/shm/mapping"
	"hash/crc32"
	"reflect"
	"sync/atomic"
	"unsafe"
)

// Map is a shared map
type Map struct {
	mp   *mapping.Mapping
	head *header
	hash *[maxMapCap]hash
	data uintptr
}

// header in database
type header struct {
	len        int32
	cap        int32
	keySize    int32
	bucketSize int32
	hashOff    uint32
	dataOff    uint32
	lock       int32
	next       int32
	deleteLink int32
	_          [7]int32
}

// hash as [4]int32
// 1st for index
// 2nd for serial
// 3rd for lock
// 4th leave blank
type hash [4]int32

// bucket header
type bucket struct {
	next int32
	hash int32
	used int32
	_    int32
	// key [keySize]byte
	// value [bucketSize]byte
}

const (
	maxMapCap  = 64 * 1024 * 1024
	minKeySize = 8
	maxKeySize = 256
	maxBktSize = 4096
	maxTries   = 20
)

var (
	// ErrMapCap on param validate
	ErrMapCap = errors.New("map cap too large or too small")
	// ErrKeyLen on param validate
	ErrKeyLen = errors.New("key too long or too short")
	// ErrValLen on param validate
	ErrValLen = errors.New("value too large or too small")
	// ErrKeyNot on get and not add
	ErrKeyNot = errors.New("key not found in map")
	// ErrDbSize on open an exist db
	ErrDbSize = errors.New("database size mismatch")
	// ErrDbFull on add a new key
	ErrDbFull = errors.New("no more space in map")
	// ErrTryEnd on add or delete
	ErrTryEnd = errors.New("cannot add after too many tries")
)

// Create or open a shared map database
func Create(path string, mapCap, keyLen, valueLen int) (m *Map, err error) {
	var hdr header
	if mapCap <= 0 || mapCap > maxMapCap {
		err = ErrMapCap
		return
	}
	// round up to power of 2
	mapCap--
	mapCap |= mapCap >> 1
	mapCap |= mapCap >> 2
	mapCap |= mapCap >> 4
	mapCap |= mapCap >> 8
	mapCap++
	if mapCap < 8 {
		mapCap = 8
	}
	hdr.cap = int32(mapCap)
	if keyLen < minKeySize-1 || keyLen > maxKeySize-1 {
		err = ErrKeyLen
		return
	}
	// plus one byte for length
	keyLen = (keyLen + 1 + 3) & (^3)
	hdr.keySize = int32(keyLen)
	if valueLen < 0 || valueLen > maxBktSize-int(unsafe.Sizeof(bucket{}))-keyLen {
		err = ErrValLen
		return
	}
	bktLen := int(unsafe.Sizeof(bucket{})) + keyLen + valueLen
	// round up to multiples of 16
	bktLen = (bktLen + 15) & (^15)
	hdr.bucketSize = int32(bktLen)
	// hash area after header
	hdr.hashOff = uint32(unsafe.Sizeof(hdr))
	// hash area size
	hashSize := int(unsafe.Sizeof(hash{})) * mapCap
	hdr.dataOff = hdr.hashOff + uint32(hashSize)
	// total size, header + hash + buckets
	size := int(hdr.dataOff) + int(hdr.cap*hdr.bucketSize)
	mp, err := database.Open(path, size)
	if err != nil {
		return
	}
	m = &Map{
		mp: mp,
	}
	err = m.init(&hdr)
	if err != nil {
		_ = m.Close()
		return
	}
	return
}

// Close the shared map database
func (m *Map) Close() error {
	err := m.mp.Close()
	m.mp = nil
	m.head = nil
	m.hash = nil
	m.data = 0
	return err
}

// Get or add an key
// return the value in a byte slice on success
// return error on failure if !add, maybe because of:
// too many tries on a highly parallel situation, or
// no more space in the database, or
// hash func failed
func (m *Map) Get(key string, add bool) (b []byte, err error) {
	h, err := hashFunc(key)
	if err != nil {
		return
	}
	ptr := m.hashPtr(h)
	try := maxTries
	var newIdx int32
	var target *bucket
	for try > 0 {
		try--
		index := ptr.index()
		serial := ptr.serial()
		// traverse the bucket chain
		for idx := index; idx >= 0; {
			bkt := m.bucket(idx)
			if key != bkt.key() {
				idx = bkt.next
				continue
			}
			b = bkt.value(m)
			return
		}
		// not found
		if !add {
			err = ErrKeyNot
			return
		}
		if target == nil {
			newIdx = m.alloc()
			if newIdx < 0 {
				err = ErrDbFull
				return
			}
			target = m.bucket(newIdx)
			target.setKey(m, key)
			target.hash = h
		}
		// lock succeed if index and serial not are changed
		if ptr.lock(index, serial) {
			target.next = index
			ptr.setIndex(newIdx)
			target.used = 1
			ptr.unlock()
			b = target.value(m)
			return
		}
	}
	if target != nil {
		m.free(newIdx)
	}
	return nil, ErrTryEnd
}

// Delete a key
// return false on failure, maybe because of:
// too many tries on a highly parallel situation, or
// hash func failed
func (m *Map) Delete(key string) bool {
	h, err := hashFunc(key)
	if err != nil {
		return false
	}
	ptr := m.hashPtr(h)
	try := maxTries
	for try > 0 {
		try--
		index := ptr.index()
		serial := ptr.serial()
		var last, target *bucket
		// traverse the bucket chain
		idx := index
		for idx >= 0 {
			bkt := m.bucket(idx)
			if key != bkt.key() {
				last = bkt
				idx = bkt.next
				continue
			}
			target = bkt
			break
		}
		// not found
		if target == nil {
			return true
		}
		// lock succeed if index and serial not are changed
		if ptr.lock(index, serial) {
			target.used = 0
			if last != nil {
				last.next = target.next
			} else {
				ptr.setIndex(target.next)
			}
			ptr.unlock()
			m.free(idx)
			return true
		}
	}
	return false
}

// Foreach key/value pair in the map call fn
// stop on fn return false or finished
func (m *Map) Foreach(fn func(key string, value []byte) bool) {
	for i := int32(0); i < m.head.cap; i++ {
		bkt := m.bucket(i)
		if bkt.used == 0 {
			continue
		}
		if !fn(bkt.key(), bkt.value(m)) {
			return
		}
	}
}

// from a exist db, or a newly created one
func (m *Map) init(h *header) error {
	data := m.mp.Bytes()
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	head := (*header)(unsafe.Pointer(sh.Data))
	// spinlock with max try limit
	for i := 0; i < 1000000 && !atomic.CompareAndSwapInt32(&head.lock, 0, 1); i++ {
	}
	// spinlock unlock
	defer func() {
		atomic.StoreInt32(&head.lock, 0)
	}()
	if head.cap != 0 {
		// this branch opened a exist db
		if head.cap != h.cap ||
			head.keySize != h.keySize ||
			head.bucketSize != h.bucketSize ||
			head.hashOff != h.hashOff ||
			head.dataOff != h.dataOff {
			return ErrDbSize
		}
	} else {
		// new db, init hash area, set index to -1
		hs := (*[maxMapCap]hash)(unsafe.Pointer(sh.Data + uintptr(h.hashOff)))
		for i := 0; i < int(h.cap); i++ {
			(*hs)[i][0] = -1
		}
		// set deleted link to -1
		head.deleteLink = -1
		// copy header params
		head.keySize = h.keySize
		head.bucketSize = h.bucketSize
		head.hashOff = h.hashOff
		head.dataOff = h.dataOff
		// set cap at the end
		head.cap = h.cap
	}
	m.head = head
	m.hash = (*[maxMapCap]hash)(unsafe.Pointer(sh.Data + uintptr(head.hashOff)))
	m.data = sh.Data + uintptr(head.dataOff)
	return nil
}

// bucket index
func (m *Map) alloc() int32 {
	// from deleted first
	for {
		del := m.head.deleteLink
		if del < 0 {
			break
		}
		bkt := m.bucket(del)
		if atomic.CompareAndSwapInt32(&m.head.deleteLink, del, bkt.next) {
			bkt.next = -1
			return del
		}
	}
	// from "next" second
	for {
		next := m.head.next
		if next >= m.head.cap {
			break
		}
		if atomic.CompareAndSwapInt32(&m.head.next, next, next+1) {
			m.bucket(next).next = -1
			return next
		}
	}
	return -1
}

// bucket index
func (m *Map) free(i int32) {
	bkt := m.bucket(i)
	// put to deleted link
	for {
		last := m.head.deleteLink
		bkt.next = last
		if atomic.CompareAndSwapInt32(&m.head.deleteLink, last, i) {
			return
		}
	}
}

// index to pointer
func (m *Map) bucket(i int32) *bucket {
	return (*bucket)(unsafe.Pointer(uintptr(unsafe.Pointer(m.head)) + uintptr(m.head.dataOff+uint32(m.head.bucketSize*i))))
}

// pointer to index
func (m *Map) index(b *bucket) int32 {
	return int32(uint32(uintptr(unsafe.Pointer(b))-uintptr(unsafe.Pointer(m.head)))-m.head.dataOff) / m.head.bucketSize
}

// hash pointer
func (m *Map) hashPtr(h int32) *hash {
	return &(*m.hash)[int(uint(h)%uint(m.head.cap))]
}

// the first bucket's index in chain
func (h *hash) index() int32 {
	return (*h)[0]
}

// serial number for change check
func (h *hash) serial() int32 {
	return (*h)[1]
}

// set the first bucket index of chain
func (h *hash) setIndex(index int32) {
	(*h)[0] = index
}

// lock the bucket chain
func (h *hash) lock(index, serial int32) bool {
	if atomic.CompareAndSwapInt32(&(*h)[2], 0, 1) {
		if index == (*h)[0] && serial == (*h)[1] {
			return true
		}
		atomic.StoreInt32(&(*h)[2], 0)
	}
	return false
}

// unlock the bucket chain
func (h *hash) unlock() {
	(*h)[1]++
	atomic.StoreInt32(&(*h)[2], 0)
}

// find chain
func (b *bucket) hashPtr(m *Map) *hash {
	return m.hashPtr(b.hash)
}

// bucket key
func (b *bucket) key() (s string) {
	a := uintptr(unsafe.Pointer(b)) + unsafe.Sizeof(bucket{})
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	h.Data = a + 1
	h.Len = int(*(*uint8)(unsafe.Pointer(a)))
	return
}

// bucket value
func (b *bucket) value(m *Map) (d []byte) {
	a := uintptr(unsafe.Pointer(b)) + unsafe.Sizeof(bucket{}) + uintptr(m.head.keySize)
	h := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	h.Data = a
	h.Cap = int(m.head.bucketSize) - int(unsafe.Sizeof(bucket{})) - int(m.head.keySize)
	h.Len = h.Cap
	return
}

// set bucket key
func (b *bucket) setKey(m *Map, s string) {
	var d []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	a := uintptr(unsafe.Pointer(b)) + unsafe.Sizeof(bucket{})
	h.Data = a + 1
	h.Cap = int(m.head.keySize - 1)
	h.Len = h.Cap
	l := copy(d, s)
	*(*uint8)(unsafe.Pointer(a)) = uint8(l)
}

// string hash func
func hashFunc(s string) (int32, error) {
	var b []byte
	*(*string)(unsafe.Pointer(&b)) = s
	(*reflect.SliceHeader)(unsafe.Pointer(&b)).Cap = len(s)
	hs := crc32.NewIEEE()
	_, err := hs.Write(b)
	if err != nil {
		return 0, err
	}
	return int32(hs.Sum32()), nil
}
