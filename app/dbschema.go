package main

import (
	"bufio"
	"os"
)

var (
	HeaderRange    = [2]int{0, 100}
	BTreePageRange = [2]int{HeaderRange[1], 12}
)

type DBSchemaHeader struct {
	reader bufio.Reader
	header []byte
}

func NewDBSchemaHeader(dbFile os.File) (DBSchemaHeader, error) {
	reader := bufio.NewReader(&dbFile)
	header := make([]byte, 100)
	_, err := reader.Read(header)

	if err != nil {
		return DBSchemaHeader{}, err
	}

	return DBSchemaHeader{
		reader: *reader,
		header: header,
	}, nil
}

func (d *DBSchemaHeader) PageSize() (uint16, error) {
	var pageSize uint16
	err := ReadBinaryFromBytes(d.header[16:18], &pageSize)
	if err != nil {
		return 0, err
	}

	return pageSize, nil
}
