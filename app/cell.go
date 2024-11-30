package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
)

// Constant element sizes
const (
	CellSizeLen = 2
)

const (
	SerialTypeNull     = "NULL"
	SerialTypeInt8     = "int8"
	SerialTypeInt16    = "int16"
	SerialTypeInt24    = "int24"
	SerialTypeInt32    = "int32"
	SerialTypeInt48    = "int48"
	SerialTypeInt64    = "int64"
	SerialTypeFloat64  = "float64"
	SerialTypeZero     = "zero"
	SerialTypeOne      = "one"
	SerialTypeBlob     = "blob"
	SerialTypeText     = "text"
	SerialTypeInvalid  = "invalid"
	SerialTypeReserved = "reserved"
)

type Cell struct {
	offset int64
	file   *os.File
	// Saving intermediate results in this struct for method chaining,
	// however a separate `Cell Processor` struct could be defined instead to save the state there
	recordSize uint64
	rowId      uint64
	err        error
}

func NewCell(file *os.File, offset int64) Cell {
	cell := Cell{}
	cell.offset = offset
	cell.file = file
	return cell
}

// TODO: maybe determine position of record size, rowId and record at cell creation?
func (c *Cell) RecordSize() *Cell {
	buf := make([]byte, 9)
	_, err := c.file.ReadAt(buf, c.offset)
	if err != nil {
		c.err = errors.New("failed to read record size bytes: " + err.Error())
	}

	recordSize, recordSizeLen := binary.Uvarint(buf)
	if recordSizeLen <= 0 {
		c.err = errors.New("cell record size buf is too small or value is larger than 64-bits")
	}

	c.recordSize = recordSize
	c.err = nil
	return c
}

func (c *Cell) RowId() *Cell {
	if c.err != nil {
		return c
	}

	buf := make([]byte, 9)
	_, err := c.file.ReadAt(buf, c.offset+int64(len(binary.AppendUvarint([]byte{}, c.recordSize))))
	if err != nil {
		c.err = errors.New("failed to read row id bytes: " + err.Error())
	}

	rowId, rowIdLen := binary.Uvarint(buf)
	if rowIdLen <= 0 {
		c.err = errors.New("cell row id buf is too small or value is larger than 64-bits")
	}

	c.rowId = rowId
	c.err = nil
	return c
}

func (c *Cell) Record() Record {
	if c.err != nil {
		return Record{}
	}

	recordData := make([]byte, c.recordSize)
	_, err := c.file.ReadAt(recordData, c.offset+int64(len(binary.AppendUvarint([]byte{}, c.recordSize)))+int64(len(binary.AppendUvarint([]byte{}, c.rowId))))
	if err != nil {
		c.err = err
	}
	// fmt.Println("RECORD SIZE:", c.recordSize)
	c.err = nil
	// ? Maybe initialize the record without all its data and read it on demand?
	return Record{data: recordData}
}

// GetProperties returns the values of recordSize and rowId of the cell
// at call time, no computations are performed.
func (c *Cell) GetProperties() (recordSize uint64, rowId uint64) {
	return c.recordSize, c.rowId
}

func (c *Cell) GetError() error {
	return c.err
}

type Record struct {
	data       []byte
	headerSize uint64
	err        error
}

func (r *Record) HeaderSize() *Record {
	headerSize, headerSizeLen := binary.Uvarint(r.data)
	if headerSizeLen <= 0 {
		r.err = errors.New("header record is too small or value is larger than 64-bits")
		return r
	}

	// fmt.Printf("header - header len: %d, size: %d, bytes: %v\n", headerSizeLen, headerSize, r[:headerSize])
	r.headerSize = headerSize
	r.err = nil
	return r
	// return r.data[:headerSize], headerSizeLen, nil
}

func (r *Record) Header() []byte {
	if r.err != nil {
		return []byte{}
	}
	return r.data[:r.headerSize]
}

func (r *Record) Data() []byte {
	return r.data
}

func (r *Record) Fields() ([][]byte, []FieldParser, error) {
	if r.err != nil {
		return [][]byte{}, []FieldParser{}, r.err
	}

	fields := [][]byte{}
	fieldParsers := []FieldParser{}
	accumOffset := uint64(0)
	body := r.data[r.headerSize:]
	bodyLen := uint64(len(body))
	headerReader := bytes.NewReader(r.data[len(binary.AppendUvarint([]byte{}, r.headerSize)):len(r.Header())]) // read starting from the non-header size bytes
	// fmt.Println("header to read", r[headerByteLen:len(header)])
	// fmt.Printf("body - size: %v bytes: %v", bodyLen, body)

	fmt.Println("body length", bodyLen)

	for {
		serialType, err := binary.ReadUvarint(headerReader)
		if err != nil {
			if err != io.EOF {
				return [][]byte{}, []FieldParser{}, errors.New("failed to read serial type varint from header: " + err.Error())
			}
			break
		}

		contentSize, fieldParser, ok := SerialTypeInfo(serialType)
		if !ok {
			return [][]byte{}, []FieldParser{}, fmt.Errorf("invalid serial type in record: %d", serialType)
		}

		// fmt.Printf("serialType %v, dataType %v, content size %v\n", serialType, fieldParser.dataType, contentSize)

		upperBound := accumOffset + contentSize
		// ? For some reason, the last content size is huge in some cases. This condition prevents out of bounds errors
		if upperBound > bodyLen {
			upperBound = bodyLen
		}
		/* if accumOffset >= bodyLen {
			accumOffset = bodyLen
		}
		*/
		fields = append(fields, body[accumOffset:upperBound])
		fieldParsers = append(fieldParsers, fieldParser)
		accumOffset += contentSize
	}

	return fields, fieldParsers, nil
}

func (r *Record) GetError() error {
	return r.err
}

// ReadCellOffset returns a cell offset by reading the next CellSizeLen bytes
// at offset off. It does not advance the underlying reader.
// Clients are responsible for providing the correct offset off.
func ReadCellOffset(dbFile *os.File, off int64) (int16, error) {
	cellOffset := int16(0)
	buf := make([]byte, CellSizeLen)
	_, err := dbFile.ReadAt(buf, off)
	if err != nil {
		return 0, errors.New("failed to read CellSizeLen bytes: " + err.Error())
	}

	err = ReadBinaryFromBytes(buf, &cellOffset)
	if err != nil {
		return 0, errors.New("failed to assign cell offset " + err.Error())
	}

	return cellOffset, nil
}

type FieldParser struct {
	dataType string
	Parse    func(data []byte) (interface{}, error)
}

// SerialTypeContentSize returns the content size of an input serial type varint
// according to the SQLite "Serial Type Codes Of The Record Format" table
func SerialTypeInfo(serialInt uint64) (size uint64, parser FieldParser, valid bool) {
	switch serialInt {
	case 0:
		return 0, FieldParser{SerialTypeNull, ParseNull}, true
	case 1:
		return 1, FieldParser{SerialTypeInt8, CreateParserFunction(ParseInt8)}, true
	case 2:
		return 2, FieldParser{SerialTypeInt16, CreateParserFunction(ParseInt16)}, true
	case 3:
		return 3, FieldParser{SerialTypeInt24, CreateParserFunction(ParseInt24)}, true
	case 4:
		return 4, FieldParser{SerialTypeInt32, CreateParserFunction(ParseInt32)}, true
	case 5:
		return 6, FieldParser{SerialTypeInt48, CreateParserFunction(ParseInt48)}, true
	case 6:
		return 8, FieldParser{SerialTypeInt64, CreateParserFunction(ParseInt64)}, true
	case 7:
		return 8, FieldParser{SerialTypeFloat64, CreateParserFunction(ParseFloat64)}, true
	case 8:
		return 0, FieldParser{SerialTypeZero, CreateParserFunction(ParseZero)}, true
	case 9:
		return 0, FieldParser{SerialTypeOne, CreateParserFunction(ParseOne)}, true
	case 10, 11:
		return 0, FieldParser{SerialTypeReserved, CreateParserFunction(ParseReserved)}, true
	default:
		if serialInt >= 12 && serialInt%2 == 0 {
			return (serialInt - 12) / 2, FieldParser{SerialTypeBlob, CreateParserFunction(ParseBlob)}, true
		}
		if serialInt >= 13 && serialInt%2 != 0 {
			return (serialInt - 13) / 2, FieldParser{SerialTypeText, CreateParserFunction(ParseText)}, true
		}
	}

	return 0, FieldParser{SerialTypeInvalid, CreateParserFunction(ParseInvalid)}, false
}

/* func ParseCellBytes(cellBytes []byte, serialType string) interface{} {}
} */
// TODO: implement a parser per type and return a struct
// with type and parser properties in the above function:
// type CellParser struct {
//  dataType SerialType
//  data     []byte
//}

// UnmarshalFieldBytes takes a field slice and a pointer to a struct v and attempts to unmarshal
// the bytes into the fields specified in type v. The struct v should be empty, otherwise
// its contents will be overriden by the content of fields. Fields should be in the same order as
// the database schema.
func UnmarshalFieldBytes[T any](fields [][]byte, v *T) error {
	// value := reflect.ValueOf(v)
	valueType := reflect.TypeOf(v)
	numFields := valueType.NumField()
	valueMap := map[string]interface{}{}

	for i := 0; i < numFields; i++ {
		fieldName := valueType.Field(i).Name
		valueMap[fieldName] = string(fields[i])
	}

	b, err := json.Marshal(valueMap)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, v)
	if err != nil {
		return err
	}
	return nil
}
