package sstable

import (
	"bytes"
	"encoding/binary"
)

/*
Теперь и индексные блоки, и блоки данных содержат индексированные смещения. Однако важно понимать, что эти смещения имеют два разных значения. Внутри индексного блока ключ каждой индексной записи говорит нам о том, что все ключи <= определенного ключа находятся в конкретном блоке данных:

Поэтому при выполнении двоичного поиска мы хотим найти самый правый блок данных, где searchKey <= largestDataBlockKey. Например, если мы ищем ключ lorem, то с помощью двоичного поиска найдем блок данных 1, поскольку ключи, хранящиеся в блоке данных 1, > foo и <= ipsum, а foo < lorem < ipsum.

Однако внутри наших блоков данных каждое смещение указывает на самый первый ключ блока данных, а самый первый ключ блока данных говорит нам, что ключи >= этого ключа находятся либо в этом, либо в одном из следующих блоков данных:

Поэтому при выполнении двоичного поиска мы хотим найти самый левый кусок данных, где firstKey > searchKey, так как searchKey будет находиться где-то в непосредственно предшествующем куске данных. На примере иллюстрации выше, если мы ищем ключ car, мы используем двоичный поиск, чтобы найти чанк данных 2, чтобы извлечь чанк данных 1, который содержит фактический ключ (потому что bar < car < culpa и ключи, хранящиеся в чанке данных 1, >= bar и < culpa).

*/

type searchCondition int

/*
Для поиска в индексном блоке мы просто вызываем index.search(searchKey, moveUpWhenKeyGT), а для поиска в блоках данных используем data.search(searchKey, moveUpWhenKeyGTE).

Теперь нет необходимости выполнять последовательный поиск по всему блоку данных. Вместо этого мы можем выполнить двоичный поиск по индексированным смещениям в блоке данных, найти нужный фрагмент данных и затем выполнить только последовательный поиск в этом фрагменте.

*/
const (
	moveUpWhenKeyGTE searchCondition = iota
	moveUpWhenKeyGT
)

type readerBlock struct {
	buf        []byte
	offsets    []byte
	numOffsets int
}

func (b *readerBlock) readOffsetAt(pos int) int {
	offset, _, _ := b.fetchDataFor(pos)
	return offset
}

func (b *readerBlock) readKeyAt(pos int) []byte {
	_, key, _ := b.fetchDataFor(pos)
	return key
}

// Отдаст начала смещения в блоке данных и крайний индекс границы блока данных (8 байт)
func (b *readerBlock) readValAt(pos int) []byte {
	_, _, val := b.fetchDataFor(pos)
	return val
}

// Чтение индексного блока с крайними ключами и общей длиной блока данных (entries)
func (b *readerBlock) fetchDataFor(pos int) (kvOffset int, key, val []byte) {
	var keyLen, valLen uint64
	var n int
	kvOffset = int(binary.LittleEndian.Uint32(b.offsets[pos*4 : pos*4+4]))
	offset := kvOffset
	
	_, n = binary.Uvarint(b.buf[offset:]) // sharedLen = 0
	offset += n
	
	keyLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	
	valLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	
	key = b.buf[offset : offset+int(keyLen)]
	offset += int(keyLen)
	val = b.buf[offset : offset+int(valLen)]
	
	return
}

// По ключу в оффсетах находит номер оффсета блока данных
func (b *readerBlock) search(searchKey []byte, condition searchCondition) int {
	low, high := 0, b.numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		key := b.readKeyAt(mid)
		cmp := bytes.Compare(searchKey, key)
		if cmp >= int(condition) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
