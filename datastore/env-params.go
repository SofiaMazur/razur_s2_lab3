package datastore

import "fmt"

const (
	outFileName = "current-data"
	containerName = "container"
	MaxFileSizeMb = 10
)

type hashIndex map[string]int64
type indexes map[string]hashIndex

var (
	ErrNotFound = fmt.Errorf("record does not exist")
	ErrHashSums = fmt.Errorf("hash sums don't match")
)
