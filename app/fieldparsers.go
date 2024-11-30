package main

import "errors"

func CreateParserFunction[T interface{}](f func(b []byte) (T, error)) func([]byte) (interface{}, error) {
	return func(b []byte) (interface{}, error) { return f(b) }
}

// Parser functions for each SerialType
func ParseNull(data []byte) (interface{}, error) {
	return nil, nil
}

func ParseZero(data []byte) (int, error) {
	return 0, nil
}

func ParseOne(data []byte) (int, error) {
	return 1, nil
}

func ParseInt8(data []byte) (int8, error) {
	var n int8
	err := ReadBinaryFromBytes(data[:1], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseInt16(data []byte) (int16, error) {
	var n int16
	err := ReadBinaryFromBytes(data[:2], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseInt24(data []byte) (int32, error) {
	var n int32
	err := ReadBinaryFromBytes(data[:3], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseInt32(data []byte) (int32, error) {
	var n int32
	err := ReadBinaryFromBytes(data[:4], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseInt48(data []byte) (int64, error) {
	var n int64
	err := ReadBinaryFromBytes(data[:6], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseInt64(data []byte) (int64, error) {
	var n int64
	err := ReadBinaryFromBytes(data[:8], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseFloat64(data []byte) (float64, error) {
	var n float64
	err := ReadBinaryFromBytes(data[:8], &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ParseReserved(data []byte) (interface{}, error) {
	// Reserved type, should not be parsed
	return nil, errors.New("reserved value")
}

func ParseBlob(data []byte) ([]byte, error) {
	return data, nil
}

func ParseText(data []byte) (string, error) {
	return string(data), nil
}

func ParseInvalid(data []byte) (interface{}, error) {
	return nil, errors.New("invalid serial type")
}
