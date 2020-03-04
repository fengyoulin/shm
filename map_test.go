package shm

import (
	"encoding/hex"
	"math/rand"
	"os"
	"testing"
	"time"
	"unsafe"
)

const (
	testFileName = "testdb.db"
	testMapCap   = 4 * 1024 * 1024
	testValLen   = 8
	initWait     = 10 * time.Second
)

var testMap *Map

func TestCreate(t *testing.T) {
	var err error
	testMap, err = Create(testFileName, testMapCap, 2*testValLen, testValLen, initWait)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMap_GetOrAdd(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < testMapCap; i++ {
		var vb [testValLen]byte
		var kb [2 * testValLen]byte
		*(*int64)(unsafe.Pointer(&vb)) = rand.Int63()
		ks := kb[:]
		hex.Encode(ks, vb[:])
		k := *(*string)(unsafe.Pointer(&ks))
		v, err := testMap.Get(k, true)
		if err != nil {
			t.Error(err)
		}
		copy(v, vb[:])
	}
}

func TestMap_Foreach(t *testing.T) {
	fn := func(key string, value []byte) bool {
		var kb [16]byte
		ks := kb[:]
		hex.Encode(ks, value[:8])
		//k := *(*string)(unsafe.Pointer(&ks))
		//fmt.Printf("key: %s, value: %s\n", key, k)
		return true
	}
	testMap.Foreach(fn)
}

func TestMap_Delete(t *testing.T) {
	fn := func(key string, value []byte) bool {
		if !testMap.Delete(key) {
			t.Errorf("failed to delete %s\n", key)
			return false
		}
		return true
	}
	testMap.Foreach(fn)
	fn = func(key string, value []byte) bool {
		t.Errorf("found after delete: %s\n", key)
		return true
	}
	testMap.Foreach(fn)
}

func TestMap_Close(t *testing.T) {
	err := testMap.Close()
	if err != nil {
		t.Error(err)
	}
	err = os.Remove(testFileName)
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkMap_GetOrAdd(b *testing.B) {
	// create
	var err error
	testMap, err = Create(testFileName, testMapCap, 2*testValLen, testValLen, initWait)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var vb [testValLen]byte
			var kb [2 * testValLen]byte
			*(*int64)(unsafe.Pointer(&vb)) = rand.Int63()
			ks := kb[:]
			hex.Encode(ks, vb[:])
			k := *(*string)(unsafe.Pointer(&ks))
			v, err := testMap.Get(k, true)
			if err != nil {
				panic(err)
			}
			copy(v, vb[:])
		}
	})
	b.StopTimer()
	// close
	err = testMap.Close()
	if err != nil {
		b.Error(err)
	}
	err = os.Remove(testFileName)
	if err != nil {
		b.Error(err)
	}
}
