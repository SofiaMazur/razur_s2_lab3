package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{key: "key", value: "value"}
	hashSum := getHashSum(e.key, e.value)
	e.sum = hashSum
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
	if e.sum != hashSum {
		t.Error("incorrect hash sum")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{key: "key", value: "test-value"}
	e.sum = getHashSum(e.key, e.value)
	data := e.Encode()
	v, hash, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bat value [%s]", v)
	}
	if err := compareHash(e.key, e.value, hash); err != nil {
		t.Fatal(err)
	}
}
