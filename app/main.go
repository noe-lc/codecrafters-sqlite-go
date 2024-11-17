package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	dbFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		dbFile, err := os.Open(dbFilePath)
		if err != nil {
			log.Fatal(err)
		}

		dbHeader, err := NewDBSchemaHeader(dbFile)
		if err != nil {
			log.Fatal(err)
		}

		pageSize, _ := dbHeader.PageSize()

		fmt.Printf("database page size: %v\n", pageSize)

		_, err = dbFile.Seek(int64(HeaderRange[1]), io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}
		bTreePageHeader := make([]byte, BTreePageRange[1])
		_, err = dbFile.Read(bTreePageHeader)
		if err != nil {
			log.Fatal(err)
		}

		var numberOfCells uint16
		err = ReadBinaryFromBytes(bTreePageHeader[3:5], &numberOfCells)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		fmt.Printf("number of tables: %v\n", numberOfCells)
	case ".tables":
		dbFile, err := os.Open(dbFilePath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = dbFile.Seek(int64(HeaderRange[1]), io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}

		bTreePageFlag := make([]byte, 1)
		_, err = dbFile.ReadAt(bTreePageFlag, int64(HeaderRange[1]))
		if err != nil {
			log.Fatal(err)
		}
		_, bTreePgHeaderSize := BTreePageTypeAndSize(bTreePageFlag[0])

		if bTreePgHeaderSize == 0 {
			log.Fatal("invalid b tree page type")
		}

		bTreePageHeader := make([]byte, bTreePgHeaderSize)
		_, err = dbFile.Read(bTreePageHeader) // reading the header positions pointer right at cell pointer array
		if err != nil {
			log.Fatal(err)
		}

		var numberOfCells uint16
		err = ReadBinaryFromBytes(bTreePageHeader[3:5], &numberOfCells)
		if err != nil {
			fmt.Println("Failed to read number of cells integer:", err)
			return
		}
		// fmt.Printf("number of tables: %v\n", numberOfCells)

		tableNames := ""
		cellStartOffset := int64(HeaderRange[1] + bTreePgHeaderSize)

		for n := range numberOfCells {
			// fmt.Println("ITERATING CELL: ", n)

			// set pointer to cell pointer position
			dbFile.Seek(cellStartOffset+int64(n*OffsetByteLen), io.SeekStart)

			cellOffset := int16(0)
			offsetBytes := make([]byte, OffsetByteLen)
			_, err := dbFile.Read(offsetBytes /* accumOffset */)

			if err != nil {
				log.Fatal(err)
			}

			err = ReadBinaryFromBytes(offsetBytes, &cellOffset)
			if err != nil {
				fmt.Println("Failed to read offset for cell", ": ", err)
				log.Fatal(err)
			}
			// fmt.Println("offset: ", cellOffset)

			//** ---  read cells
			_, err = dbFile.Seek(int64(cellOffset), io.SeekStart) // set offset for cell read
			if err != nil {
				fmt.Println("unable to set offset for cell", err)
				continue
			}

			// 1 record size
			buf := make([]byte, 9)
			_, err = dbFile.ReadAt(buf, io.SeekCurrent)
			if err != nil {
				fmt.Println("unable to read record size for cell ", err)
				continue
			}
			recordSize, recordSizeLen := binary.Uvarint(buf)
			if recordSizeLen <= 0 {
				fmt.Println("buf is too small or value is larger than 64-bits for cell ", n)
				continue
			}
			// fmt.Println("Record size and len: ", recordSize, recordSizeLen)

			// 2 rowid
			_, err = dbFile.Seek(int64(recordSizeLen), io.SeekCurrent)

			if err != nil {
				fmt.Println("unable to set offset for cell rowid", n)
				continue
			}

			buf = make([]byte, 9)
			_, err = dbFile.ReadAt(buf, io.SeekCurrent)
			if err != nil {
				fmt.Println("unable to read bytes for row id for cell", n)
				continue
			}
			_, rowIdLen := binary.Uvarint(buf)
			if rowIdLen <= 0 {
				fmt.Println("row id buf is too small or value is larger than 64-bits for cell ", n)
				continue
			}
			// fmt.Println("Row id: ", rowId)

			// 3 record
			dbFile.Seek(int64(rowIdLen), io.SeekCurrent)

			record := make([]byte, recordSize)
			_, err = dbFile.Read(record)
			if err != nil {
				fmt.Println("failed to read record: ", err)
				continue
			}

			headerSize, headerSizeLen := binary.Uvarint(record)
			if headerSizeLen <= 0 {
				fmt.Println("header buf is too small or value is larger than 64-bits for cell")
				continue
			}
			// fmt.Println("header size:", headerSize)

			recordHeader := bytes.NewReader(record[headerSizeLen:headerSize])
			recordBody := record[headerSize:]
			recordContentSizes := []uint64{}

			for {
				serialType, err := binary.ReadUvarint(recordHeader)

				if err != nil {
					if err == io.EOF {
						// fmt.Println("Finished reading record header")
					} else {
						fmt.Println("failed to read serial type")
					}
					break
				}

				contentSize, ok := SerialTypeData(serialType)

				if !ok {
					fmt.Println("invalid serial type ", serialType)
					recordContentSizes = append(recordContentSizes, 0)
				} else {
					recordContentSizes = append(recordContentSizes, contentSize)
				}

				// fmt.Println("serial type, contentSize ", serialType, contentSize)
			}

			offsetToTblName := recordContentSizes[0] + recordContentSizes[1]
			tableNames += string(recordBody[offsetToTblName:offsetToTblName+recordContentSizes[2]]) + " "

		}

		fmt.Println(tableNames)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
