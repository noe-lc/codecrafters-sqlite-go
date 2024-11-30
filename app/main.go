package main

import (
	"fmt"
	"log"
	"os"
	"strings"
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

		defer dbFile.Close()

		dbHeader := NewDBSchemaHeader(dbFile)
		pageSize, _ := dbHeader.PageSize()

		fmt.Printf("database page size: %v\n", pageSize)

		bTreePageHeader, err := NewBTreePageHeader(dbFile, int64(DBHeaderSection.Size))
		if err != nil {
			log.Fatal(err)
		}
		cells, err := bTreePageHeader.Cells(int64(DBHeaderSection.Size))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("number of tables: %v\n", len(cells))
	case ".tables":
		dbFile, err := os.Open(dbFilePath)
		if err != nil {
			log.Fatal(err)
		}

		defer dbFile.Close()

		tableNames := ""
		fields, fieldParsers, err := TableRows(dbFile, int64(DBHeaderSection.Size))
		if err != nil {
			log.Fatal(err)
		}

		for i, f := range fields {
			nameField := f[2]
			nameParser := fieldParsers[i][2]
			tableName, _ := nameParser.Parse(nameField)
			tableNames += tableName.(string) + " "
		}

		fmt.Println(tableNames)

	default:
		// TODO: the root page field is within the sqlite_schema table (read also by .tables)
		// In .tables, you read the 3rd field from the record body, rootpage is the 4th

		// * handling table name as last item for now
		commandParts := strings.Split(command, " ")
		inputTableName := commandParts[len(commandParts)-1]
		dbFile, err := os.Open(dbFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer dbFile.Close()

		tableRootPage := -1
		rows, rowParsers, err := TableRows(dbFile, int64(DBHeaderSection.Size))
		if err != nil {
			log.Fatal(err)
		}

		for i, f := range rows {
			nameField := f[2]
			nameParser := rowParsers[i][2]
			tableName, _ := nameParser.Parse(nameField)

			if tableName == inputTableName {
				rootPageField := f[3]
				rootPageParser := rowParsers[i][3]
				rootPage, err := rootPageParser.Parse(rootPageField)
				if err != nil {
					log.Fatal(err)
				}
				tableRootPage = int(rootPage.(int8))
				break
			}
		}

		if tableRootPage == -1 {
			log.Fatal("table does not exist")
		}

		dbHeader := NewDBSchemaHeader(dbFile)
		pageSize, err := dbHeader.PageSize()
		if err != nil {
			log.Fatal(err)
		}

		// requested table
		tableOffset := int(pageSize) * (tableRootPage - 1)
		rowCount, err := RowCount(dbFile, int64(tableOffset))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(rowCount)
	}
}
