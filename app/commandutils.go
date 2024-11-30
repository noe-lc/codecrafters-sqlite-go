package main

import (
	"os"
)

func TableRows(dbFile *os.File, pageHeaderOffset int64) ([][][]byte, [][]FieldParser, error) {
	bTreePageHeader, err := NewBTreePageHeader(dbFile, pageHeaderOffset)
	if err != nil {
		return [][][]byte{}, [][]FieldParser{}, err
	}

	cells, err := bTreePageHeader.Cells(pageHeaderOffset)
	if err != nil {
		return [][][]byte{}, [][]FieldParser{}, err
	}

	tableFields := [][][]byte{}
	tableFieldParsers := [][]FieldParser{}

	for _, cell := range cells {
		// fmt.Println("ITERATING CELL: ", n)
		//** ---  read cells
		record := cell.RecordSize().RowId().Record()
		err := cell.GetError()
		if err != nil {
			return [][][]byte{}, [][]FieldParser{}, err
		}
		// fmt.Println("record ", record)

		fields, fieldParsers, err := record.HeaderSize().Fields()
		if err != nil {
			return [][][]byte{}, [][]FieldParser{}, err
		}

		tableFields = append(tableFields, fields)
		tableFieldParsers = append(tableFieldParsers, fieldParsers)
	}

	return tableFields, tableFieldParsers, nil
}

func RowCount(dbFile *os.File, pageHeaderOffset int64) (int, error) {
	bTreePageHeader, err := NewBTreePageHeader(dbFile, pageHeaderOffset)
	if err != nil {
		return 0, err
	}

	cells, err := bTreePageHeader.Cells(pageHeaderOffset)
	if err != nil {
		return 0, err
	}

	return len(cells), nil
}
