package main

import (
	"errors"
	"os"
)

type DBSection struct {
	Offset int
	Size   int
}

var DBHeaderSection = DBSection{
	Offset: 0,
	Size:   100,
}

// B-Tree Page type and sizes
const (
	InteriorIndex = "INTERIOR_INDEX"
	InteriorTable = "INTERIOR_TABLE"
	LeafIndex     = "LEAF_INDEX"
	LeafTable     = "LEAF_TABLE"
)

type DBSchemaHeader struct {
	file *os.File
}

func ByteRangeFrom(s DBSection, offset int64, size int64) (int64, int64) {
	sectionOffset := int64(s.Offset)
	return sectionOffset + offset, sectionOffset + size
}

func NewDBSchemaHeader(dbFile *os.File) DBSchemaHeader {
	return DBSchemaHeader{file: dbFile}
}

func (d DBSchemaHeader) read(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := d.file.ReadAt(buf, int64(DBHeaderSection.Offset)+offset)
	if err != nil {
		return []byte{}, err
	}
	return buf, nil
}

func (d DBSchemaHeader) PageSize() (uint16, error) {
	var pageSize uint16
	pageSizeBuf, err := d.read(16, 2)
	if err != nil {
		return 0, err
	}
	err = ReadBinaryFromBytes(pageSizeBuf, &pageSize)
	if err != nil {
		return 0, err
	}
	return pageSize, nil
}

type BTreePageHeader struct {
	file        *os.File
	startOffset int64
	pageType    string
	size        uint8
}

func NewBTreePageHeader(dbFile *os.File, startOffset int64) (BTreePageHeader, error) {
	pageFlag := make([]byte, 1)
	_, err := dbFile.ReadAt(pageFlag, startOffset)
	if err != nil {
		return BTreePageHeader{}, err
	}
	pageType, pageSize := BTreePageInfo(pageFlag[0])
	if pageSize == 0 {
		return BTreePageHeader{}, errors.New("cannot determine b-tree page type")
	}

	return BTreePageHeader{file: dbFile, startOffset: startOffset, pageType: pageType, size: pageSize}, nil
}

func (h BTreePageHeader) read(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := h.file.ReadAt(buf, h.startOffset+offset)
	if err != nil {
		return []byte{}, err
	}
	return buf, nil
}

// Cells returns the number of cells
func (h BTreePageHeader) Cells(headerStartOffset int64) ([]Cell, error) {
	var numberOfCells uint16
	buf, err := h.read(3, 5)
	if err != nil {
		return []Cell{}, err
	}
	err = ReadBinaryFromBytes(buf, &numberOfCells)
	if err != nil {
		return []Cell{}, err
	}

	cells := []Cell{}
	cellAreaOffset := headerStartOffset + int64(h.size)
	for n := range numberOfCells {
		cellOffset, err := ReadCellOffset(h.file, cellAreaOffset+int64(n*CellSizeLen))
		if err != nil {
			return []Cell{}, err
		}
		// fmt.Println("cell offset: ", cellOffset)
		cells = append(cells, NewCell(h.file, int64(cellOffset)))
	}

	return cells, nil
}

func (h BTreePageHeader) Size() uint8 {
	return h.size
}

// BTreePageData returns the page type and its header size
func BTreePageInfo(flagByte byte) (string, uint8) {
	switch flagByte {
	case 0x02:
		return InteriorIndex, 12
	case 0x05:
		return InteriorTable, 12
	case 0x0a:
		return LeafIndex, 8
	case 0x0d:
		return LeafTable, 8
	}

	return "", 0
}
