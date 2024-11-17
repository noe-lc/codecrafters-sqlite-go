package main

import (
	"io"
	"os"
)

func CurrentFileOffset(file *os.File) (int64, error) {
	return file.Seek(0, io.SeekCurrent)
}
