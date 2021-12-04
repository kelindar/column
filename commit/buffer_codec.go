// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

// --------------------------- WriteTo ----------------------------

// WriteTo writes data to w until there's no more data to write or when an error occurs. The return
// value n is the number of bytes written. Any error encountered during the write is also returned.
func (b *Buffer) WriteTo(w io.Writer) (n int64, err error) {
	nName, err := writeBytesTo(w, toBytes(b.Column))
	if err != nil {
		return 0, err
	}

	nLast, err := writeUintTo(w, int(b.last))
	if err != nil {
		return 0, err
	}

	nHead, err := writeChunksTo(w, b.chunks)
	if err != nil {
		return 0, err
	}

	nBody, err := writeBytesTo(w, b.buffer)
	if err != nil {
		return 0, err
	}

	n += int64(nLast + nName + nHead + nBody)
	return
}

// writeBytesTo writes the string to the output buffer
func writeBytesTo(w io.Writer, v []byte) (n int, err error) {
	nSize, err := writeUintTo(w, len(v))
	if err != nil {
		return 0, err
	}

	nText, err := w.Write(v)
	if err != nil {
		return 0, err
	}

	n += nSize + nText
	return
}

// writeChunksTo writes a header with chunk offsets
func writeChunksTo(w io.Writer, chunks []header) (n int, err error) {
	m, err := writeUintTo(w, len(chunks))
	if err != nil {
		return 0, err
	}

	n += m
	var temp [12]byte
	for _, v := range chunks {
		binary.BigEndian.PutUint32(temp[0:4], v.Chunk)
		binary.BigEndian.PutUint32(temp[4:8], v.Start)
		binary.BigEndian.PutUint32(temp[8:12], v.Value)
		m, err := w.Write(temp[:])
		if err != nil {
			return 0, err
		}

		n += m
	}
	return
}

// writeUintTo writes the length of something into the destination writer
func writeUintTo(w io.Writer, v int) (n int, err error) {
	var temp [4]byte
	binary.BigEndian.PutUint32(temp[:], uint32(v))
	return w.Write(temp[:])
}

// --------------------------- ReadFrom ----------------------------

// ReadFrom reads data from r until EOF or error. The return value n is the number of
// bytes read. Any error except EOF encountered during the read is also returned.
func (b *Buffer) ReadFrom(r io.Reader) (n int64, err error) {
	name, nName, err := readBytesFrom(r)
	if err != nil {
		return 0, err
	}

	last, nLast, err := readUintFrom(r)
	if err != nil {
		return 0, err
	}

	head, nHead, err := readChunksFrom(r)
	if err != nil {
		return 0, err
	}

	body, nBody, err := readBytesFrom(r)
	if err != nil {
		return 0, err
	}

	b.Column = string(name)
	b.chunks = head
	b.buffer = body
	b.last = int32(last)
	if len(head) > 0 {
		last := head[len(head)-1]
		b.chunk = last.Chunk
	}

	n += int64(nName + nLast + nHead + nBody)
	return
}

// readBytesFrom reads the bytes prefixed with the length from the reader
func readBytesFrom(r io.Reader) (v []byte, n int, err error) {
	size, nSize, err := readUintFrom(r)
	if err != nil {
		return nil, 0, err
	}

	v = make([]byte, size)
	n, err = io.ReadFull(r, v)
	n += nSize
	return
}

// readChunksFrom reads the list of chunks from the reader
func readChunksFrom(r io.Reader) (v []header, n int, err error) {
	size, m, err := readUintFrom(r)
	if err != nil {
		return nil, 0, err
	}

	n += m
	v = make([]header, size)
	var temp [12]byte
	for i := 0; i < size; i++ {
		m, err := io.ReadFull(r, temp[:])
		if err != nil {
			return nil, 0, err
		}

		v[i].Chunk = binary.BigEndian.Uint32(temp[0:4])
		v[i].Start = binary.BigEndian.Uint32(temp[4:8])
		v[i].Value = binary.BigEndian.Uint32(temp[8:12])
		n += m
	}
	return
}

// readUintFrom reads the unsigned integer from the reader
func readUintFrom(r io.Reader) (v int, n int, err error) {
	var temp [4]byte
	n, err = io.ReadFull(r, temp[:])
	v = int(binary.BigEndian.Uint32(temp[:]))
	return
}

// toBytes converts a string to a byte slice without allocating.
func toBytes(v string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(v)
	byteHeader.Len = l
	byteHeader.Cap = l
	return
}
