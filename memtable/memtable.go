package memtable

import (
	"github.com/wubba-com/lsm-tree/memtable/encoder"
	"github.com/wubba-com/lsm-tree/skiplist"
)

// https://www.cloudcentric.dev/exploring-memtables/

type Memtable struct {
	sl        *skiplist.SkipList
	encoder   *encoder.Encoder
	sizeUsed  int // The approximate amount of space used by the Memtable so far (in bytes).
	sizeLimit int // The maximum allowed size of the Memtable (in bytes).
}

func NewMemtable(sizeLimit int) *Memtable {
	m := &Memtable{
		sl:        skiplist.NewSkipList(),
		sizeLimit: sizeLimit,
	}
	return m
}

func (m *Memtable) Get(key []byte) (*encoder.EncodedValue, error) {
	v, err := m.sl.Find(key)
	if err != nil {
		return nil, err
	}

	return m.encoder.Parse(v), nil
}

func (m *Memtable) HasRoomForWrite(key, val []byte) bool {
	sizeNeeded := len(key) + len(val)
	sizeAvailable := m.sizeLimit - m.sizeUsed

	if sizeNeeded > sizeAvailable {
		return false
	}
	return true
}

func (m *Memtable) getSize(key, val []byte) int {
	return len(key) + len(val)
}

func (m *Memtable) Insert(key, val []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpKindSet, val))
	m.sizeUsed += m.getSize(key, val) + 1
}

func (m *Memtable) InsertTombstone(key []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpKindDelete, nil))
	m.sizeUsed += 1
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}

func (m *Memtable) Iterator() *skiplist.Iterator {
	return m.sl.Iterator()
}
