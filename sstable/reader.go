package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"

	"github.com/wubba-com/lsm-tree/memtable/encoder"
)

var ErrKeyNotFound = errors.New("key not found")

type statReaderAtCloser interface {
	Stat() (fs.FileInfo, error)
	io.ReaderAt
	io.Closer
}

type Reader struct {
	file     statReaderAtCloser
	br       *bufio.Reader
	buf      []byte
	encoder  *encoder.Encoder
	fileSize int64
}

func NewReader(file io.Reader) (*Reader, error) {
	r := &Reader{}
	r.file, _ = file.(statReaderAtCloser)
	r.br = bufio.NewReader(file)
	r.buf = make([]byte, 0, maxBlockSize)

	err := r.initFileSize()
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Извлеките размер загруженного файла *.sst.
func (r *Reader) initFileSize() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	r.fileSize = info.Size()

	return nil
}

// Получить весь индексный блок
func (r *Reader) readIndexBlock(footer []byte) (*readerBlock, error) {
	b := r.prepareBlockReader(r.buf, footer)

	// Находим оффсетс которого начинается индексный блок (r.fileSize - общая длина файла, len(b.buf) - длина всего индексного блока)
	indexOffset := r.fileSize - int64(len(b.buf))
	_, err := r.file.ReadAt(b.buf, indexOffset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (r *Reader) prepareBlockReader(buf, footer []byte) *readerBlock {
	// Общая длина индексного блока
	indexLength := int(binary.LittleEndian.Uint32(footer[:4]))

	// Кол-во ключей записаных в индексный блок
	numOffsets := int(binary.LittleEndian.Uint32(footer[4:]))

	// загрузка в буффер всего индексного блока
	buf = buf[:indexLength]

	return &readerBlock{
		buf:        buf,
		offsets:    buf[indexLength-(numOffsets+2)*4:],
		numOffsets: numOffsets,
	}
}

// Считайте нижний колонтитул *.sst в предоставленный буфер.
func (r *Reader) readFooter() ([]byte, error) {
	buf := r.buf[:footerSizeInBytes]
	footerOffset := r.fileSize - footerSizeInBytes
	_, err := r.file.ReadAt(buf, footerOffset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Чтение блока данных включая его мини-индексный блок
func (r *Reader) readDataBlock(indexEntry []byte) (*readerBlock, error) {
	var err error
	val := r.encoder.Parse(indexEntry).Value()
	offset := binary.LittleEndian.Uint32(val[:4]) // смещение блока данных в файле *.sst
	length := binary.LittleEndian.Uint32(val[4:]) // длина блока данных

	// Выделяем буффер под блок данных
	buf := r.buf[:length]

	// Загружаем блок данных в память
	_, err = r.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	b := r.prepareBlockReader(buf, buf[len(buf)-footerSizeInBytes:])

	return b, nil

	//return buf, nil
}

func (r *Reader) binarySearch(searchKey []byte) (*encoder.EncodedValue, error) {
	// Загрузить нижний колонтитул в память.
	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	// Загрузить индексный блок в память.
	idxBlock, err := r.readIndexBlock(footer)
	if err != nil {
		return nil, err
	}
	// Поиск в индексном блоке блока данных.
	pos := idxBlock.search(searchKey, moveUpWhenKeyGT)
	if pos >= idxBlock.numOffsets {
		// ключ поиска больше, чем самый большой ключ в текущем *.sst
		return nil, ErrKeyNotFound
	}
	indexEntry := idxBlock.readValAt(pos)

	// Загрузить блок данных в память.
	blockData, err := r.readDataBlock(indexEntry)
	if err != nil {
		return nil, err
	}

	// тут мы вернем оффсет у которого ключу будут > ищущего ключа
	offset := blockData.search(searchKey, moveUpWhenKeyGTE)
	if offset <= 0 {
		return nil, ErrKeyNotFound
	}

	chunkStart := blockData.readOffsetAt(offset - 1)
	chunkEnd := blockData.readOffsetAt(offset)
	chunk := blockData.buf[chunkStart:chunkEnd]

	return r.sequentialSearchChunk(chunk, searchKey)
}

func (r *Reader) sequentialSearchChunk(chunk []byte, searchKey []byte) (*encoder.EncodedValue, error) {
	var offset int
	for {
		var keyLen, valLen uint64
		var n int
		keyLen, n = binary.Uvarint(chunk[offset:])
		if n <= 0 {
			break // EOF
		}
		offset += n
		valLen, n = binary.Uvarint(chunk[offset:])
		offset += n
		key := r.buf[:keyLen]
		copy(key[:], chunk[offset:offset+int(keyLen)])
		offset += int(keyLen)
		val := chunk[offset : offset+int(valLen)]
		offset += int(valLen)
		cmp := bytes.Compare(searchKey, key)
		if cmp == 0 {
			return r.encoder.Parse(val), nil
		}
		if cmp < 0 {
			break // Key is not present in this data block.
		}
	}
	return nil, ErrKeyNotFound
}

func (r *Reader) Get(searchKey []byte) (*encoder.EncodedValue, error) {
	return r.binarySearch(searchKey)
}

func (r *Reader) Close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}
	r.file = nil
	r.br = nil

	return nil
}
