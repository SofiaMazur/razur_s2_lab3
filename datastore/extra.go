package datastore

import (
	"bufio"
	"os"
	"sort"
	"fmt"
	"crypto/sha1"
)

func searchValue(file *os.File, position int64) (string, [20]byte, error) {
		if _, err := file.Seek(position, 0); err != nil {
			return "", [20]byte{},  err
		}
		reader := bufio.NewReader(file)
		return readValue(reader)
}

func getSortedKeys(index indexes) []string {
	keys := make([]string, 0)
	for key := range index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func listStorageEntries(dir string) ([]string, error) {
	file, err := os.Open(dir)
  if err != nil {
    return nil, fmt.Errorf("failed opening directory: %s", err)
  }
  defer file.Close()
  return file.Readdirnames(0)
}

func getHashSum(key string, value string) [20]byte {
	return sha1.Sum([]byte(key + " " + value))
}

func compareHash(key string, value string, expected [20]byte) error {
	if got := getHashSum(key, value); got != expected {
		return ErrHashSums
	}
	return nil
}
