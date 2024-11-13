package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ReadBinaryFromBytes reads the structured binary data from input bytes into data
// and returns any errors returned by binary.Read. The byte order is binary.BigEndian
func ReadBinaryFromBytes[T interface{}](inputBytes []byte, data *T) error {
	if err := binary.Read(bytes.NewReader(inputBytes), binary.BigEndian, data); err != nil {
		fmt.Println("Failed to read binary data from bytes", err)
		return err
	}

	return nil
}
