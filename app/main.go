package main

import (
	"bufio"
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
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":
		dbHeader, err := NewDBSchemaHeader(*databaseFile)
		if err != nil {
			log.Fatal(err)
		}

		pageSize, _ := dbHeader.PageSize()

		fmt.Printf("database page size: %v\n", pageSize)

		dbReader := bufio.NewReader(databaseFile)
		dbReader.Discard(HeaderRange[1])
		bTreePageHeader := make([]byte, 12)
		_, err = dbReader.Read(bTreePageHeader)
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
		dbReader := bufio.NewReader(databaseFile)
		bTreePageHeader := make([]byte, 12)

		_, err := dbReader.Discard(HeaderRange[1])
		if err != nil {
			log.Fatal(err)
		}

		_, err = dbReader.Read(bTreePageHeader)
		if err != nil {
			log.Fatal(err)
		}

		var numberOfCells uint16
		err = ReadBinaryFromBytes(bTreePageHeader[3:5], &numberOfCells)
		if err != nil {
			fmt.Println("Failed to read number of cells integer:", err)
			return
		}

		cellOffsets := make([]int, numberOfCells)

		for n := range numberOfCells {
			var offset uint16
			offsetBytes := make([]byte, 2)
			_, err := dbReader.Read(offsetBytes)
			if err != nil {
				log.Fatal(err)
			}

			err = ReadBinaryFromBytes(offsetBytes, &offset)
			if err != nil {
				fmt.Println("Failed to read offset for cell", n, ": ", err)
				log.Fatal(err)
			}

			cellOffsets = append(cellOffsets, int(offset))
		}

		fmt.Println("offsets", cellOffsets)

		// either create a new reader or reset existing
		dbReader.Reset(bufio.NewReader(databaseFile))

		// bytesRead :=
		for i, offset := range cellOffsets {
			fmt.Println("cell", i, offset)

			// TODO: this could be achieved by using the io.ReaderAt interface
			if i == 0 { // discard first offset
				dbReader.Discard(offset)
				// 1 record size
				recordSizeBytes, err := dbReader.Peek(9)
				if err != nil {
					fmt.Println("unable to read record size for cell")
					continue
				}
				recordSize, recordSizeLen := binary.Varint(recordSizeBytes)
				if recordSizeLen <= 0 {
					fmt.Println("buf is too small or value is larger than 64-bits for cell ", i)
					continue
				}
				fmt.Println("Record size: ", recordSize)
				dbReader.Discard(recordSizeLen)

				// 2 rowid
				rowIdBytes, err := dbReader.Peek(9)
				if err != nil {
					fmt.Println("unable to read record size for cell", i)
					continue
				}
				rowId, rowIdLen := binary.Varint(rowIdBytes)
				if rowIdLen <= 0 {
					fmt.Println("row id buf is too small or value is larger than 64-bits for cell ", i)
					continue
				}
				fmt.Println("Row id: ", rowId)
				dbReader.Discard(rowIdLen)

				// 3 record
				record := make([]byte, recordSize)
				_, err = dbReader.Read(record)
				if err != nil {
					fmt.Println("failed to read record: ", err)
					continue
				}

				// headerReader := bytes.NewReader(record)
				// headerSize, err := binary.ReadVarint(headerReader)

				headerSize, headerSizeLen := binary.Varint(record)
				if headerSizeLen <= 0 {
					fmt.Println("header buf is too small or value is larger than 64-bits for cell")
					continue
				}
				fmt.Println("header size:", headerSize)
				header := bytes.NewBuffer(record[headerSizeLen:headerSize])

				for {
					serialType, err := binary.ReadVarint(header)

					if err != nil {
						if err == io.EOF {
							fmt.Println("Finished reading record header")
						} else {
							fmt.Println("failed to read serial type")
						}
						break
					}

					fmt.Println("serial type: ", serialType)

				}

			} else {
				dbReader.Read(offset - cellOffsets[i-1])
			}

		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
