package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v\n", pageSize)

		bTreePageHeader := make([]byte, 12)
		_, err = databaseFile.Read(bTreePageHeader)
		if err != nil {
			log.Fatal(err)
		}

		var numberOfCells uint16
		if err := binary.Read(bytes.NewReader(header[3:5]), binary.BigEndian, &numberOfCells); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		fmt.Printf("number of tables: %v\n", numberOfCells)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
