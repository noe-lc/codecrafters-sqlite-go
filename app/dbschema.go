package main

import (
	"io"
	"os"
)

var (
	HeaderRange    = [2]int{0, 100}
	BTreePageRange = [2]int{HeaderRange[1], 12}
)

// Constant element sizes
const (
	OffsetByteLen = 2
)

// B-Tree Page type and sizes
const (
	InteriorIndex = "INTERIOR_INDEX"
	InteriorTable = "INTERIOR_TABLE"
	LeafIndex     = "LEAF_INDEX"
	LeafTable     = "LEAF_TABLE"
)

// BTreePageData returns the page type and ints header size
func BTreePageTypeAndSize(flagByte byte) (string, int) {
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

type DBSchemaHeader []byte

func NewDBSchemaHeader(dbFile *os.File) (DBSchemaHeader, error) {
	currOffset, err := CurrentFileOffset(dbFile)
	if err != nil {
		return DBSchemaHeader{}, err
	}

	/* 	_, err = dbFile.Seek(0, io.SeekStart)
	   	if err != nil {
	   		return DBSchemaHeader{}, err
	   	} */

	header := make([]byte, 100)
	_, err = dbFile.Read(header)
	if err != nil {
		return DBSchemaHeader{}, err
	}

	_, err = dbFile.Seek(currOffset, io.SeekStart)

	if err != nil {
		return DBSchemaHeader{}, err
	}

	return header, nil
}

func (d *DBSchemaHeader) PageSize() (uint16, error) {
	var pageSize uint16
	err := ReadBinaryFromBytes([]byte(*d)[16:18], &pageSize)
	if err != nil {
		return 0, err
	}

	return pageSize, nil
}
