package sstable

import (
	"bytes"
	"encoding/binary"
)

type writerBlock struct {
	buf *bytes.Buffer

	offsets    []uint32
	nextOffset uint32

	chunkSize  int    // желаемые числа в каждом блоке данных
	numEntries int    // числа в текущем блоке данных
	prefixKey  []byte // префиксный ключ текущего блока данных
	currOffset uint32 // начальное смещение текущего фрагмента данных
}

func newBlockWriter(chunkSize int) *writerBlock {
	bw := &writerBlock{}
	bw.buf = bytes.NewBuffer(make([]byte, 0, maxBlockSize))
	bw.chunkSize = chunkSize
	return bw
}

func (wb *writerBlock) scratchBuf(needed int) []byte {
	// проверяем, хватает ли нам места для сохранения нового потока байт
	if needed > wb.buf.Available() {
		wb.buf.Grow(needed)
	}
	buf := wb.buf.AvailableBuffer()

	return buf[:needed]
}

func (b *writerBlock) calculateSharedLength(key []byte) int {
	sharedLen := 0
	if b.prefixKey == nil {
		b.prefixKey = key

		return sharedLen
	}

	for i := 0; i < min(len(key), len(b.prefixKey)); i++ {
		if key[i] != b.prefixKey[i] {
			break
		}
		sharedLen++
	}

	return sharedLen
}

func (b *writerBlock) add(key, val []byte) (int, error) {
	sharedLen := b.calculateSharedLength(key)
	keyLen, valLen := len(key), len(val)
	needed := 3*binary.MaxVarintLen64 + (keyLen - sharedLen) + valLen
	buf := b.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(keyLen-sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key[sharedLen:])
	copy(buf[n+keyLen-sharedLen:], val)
	used := n + (keyLen - sharedLen) + valLen
	n, err := b.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}
	b.numEntries++

	b.trackOffset(uint32(n))

	return n, nil
}

func (b *writerBlock) trackOffset(n uint32) {
	b.nextOffset += n

	if b.numEntries == b.chunkSize {
		b.offsets = append(b.offsets, b.currOffset)
		b.currOffset = b.nextOffset
		b.numEntries = 0
		b.prefixKey = nil
	}
}

func (b *writerBlock) finish() error {
	if b.prefixKey != nil {
		// Принудительный сброс последнего смещения ключа префикса.
		b.offsets = append(b.offsets, b.currOffset)
	}
	numOffsets := len(b.offsets)
	needed := (numOffsets + 2) * 4
	buf := b.scratchBuf(needed)
	for i, offset := range b.offsets {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], offset)
	}
	binary.LittleEndian.PutUint32(buf[needed-8:needed-4], uint32(b.buf.Len()+needed))
	binary.LittleEndian.PutUint32(buf[needed-4:needed], uint32(numOffsets))
	_, err := b.buf.Write(buf)
	if err != nil {
		return err
	}
	b.reset()

	return nil
}

func (b *writerBlock) reset() {
	b.nextOffset = 0
	b.currOffset = 0
	b.offsets = b.offsets[:0]
	b.numEntries = 0
	b.prefixKey = nil
}

/*
example:

insert foo (key) => bar (value)

bytes[3, 4, foo, 1, bar]

idx 0 => keyLen
idx 1 => valLen
idx 2 => key
idx 3 => optKey
idx 4 => value

total: 11 bytes
*/
// func (w *Writer) writeDataBlock(key, val []byte) (int, error) {
// 	keyLen, valLen := len(key), len(val)
// 	// needed := 4 + keyLen + valLen // 4 байта, нужно для хранения длины ключа и длины значения, по 2 байта на каждую длину
// 	needed := 2*binary.MaxVarintLen64 + keyLen + valLen
// 	buf := w.scratchBuf(needed)
// 	n := binary.PutUvarint(buf, uint64(keyLen))
// 	n += binary.PutUvarint(buf[n:], uint64(valLen))
// 	copy(buf[n:], key)
// 	copy(buf[n+keyLen:], val)
// 	used := n + keyLen + valLen

// 	n, err := w.bw.Write(buf[:used])
// 	if err != nil {
// 		return n, err
// 	}

// 	m, err := w.bw.ReadFrom(w.buf)
// 	if err != nil {
// 		return int(m), err
// 	}
// 	return int(m), nil
// }

// func (w *writerBlock) addIndexEntry(n int) {
// 	w.offsets = append(w.offsets, w.nextOffset)
// 	w.nextOffset += uint32(n)
// }

/*
Когда мы закончим обработку всех ожидающих блоков данных, writeIndexBlock "завершит" наш *.sst файл, создав индексный блок из всех записанных offsets, вычислив footer часть и добавив все в самый конец *.sst файла.
*/
// func (w *Writer) writeIndexBlock() error {
// 	numOffsets := len(w.offsets)
// 	needed := (numOffsets + 1) * footerSizeInBytes
// 	buf := w.scratchBuf(needed)
// 	for i, offset := range w.offsets {
// 		binary.LittleEndian.PutUint32(buf[i*footerSizeInBytes:i*footerSizeInBytes+footerSizeInBytes], offset)
// 	}
// 	binary.LittleEndian.PutUint32(buf[needed-footerSizeInBytes:needed], uint32(numOffsets))
// 	_, err := w.bw.Write(buf[:])
// 	if err != nil {
// 		log.Fatal(err)
// 		return err
// 	}
// 	return nil
// }
