package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, value string
	sum [20]byte
}


func (e *entry) Encode() []byte {
	header := 12
	sumSize := 20
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + header + sumSize
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl + header:], e.value)
	copy(res[kl + header + vl:], e.sum[:])
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl + 8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl + 12:kl + 12 + vl])
	e.value = string(valBuf)

	var sumBuf [20]byte
	copy(sumBuf[:], input[kl + vl + 12:kl + vl + 12 + 20])
	e.sum = sumBuf
}

func readValue(in *bufio.Reader) (string, [20]byte, error) {
	header, err := in.Peek(8)
	var sumBuf [20]byte

	if err != nil {
		return "", sumBuf, err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", sumBuf, err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", sumBuf, err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", sumBuf, err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", sumBuf, err
	}
	if n != valSize {
		return "", sumBuf, fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	size, err := in.Read(sumBuf[:])
	if err != nil {
		return "", sumBuf, err
	}
	if size != 20 {
		return "", sumBuf, fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), sumBuf, nil
}
