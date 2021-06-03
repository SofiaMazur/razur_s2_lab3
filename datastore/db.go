package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Db struct {
	out          *os.File
	outOffset    int64
	maxSize      int64
	params       *storageEntries
	mergeHandler *MergeHandler
	writeHandler *WriteHandler
	mtx          sync.Mutex
}

func NewDb(dir string, sizeBytes int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	list, err := listStorageEntries(dir)
	if err != nil {
		return nil, err
	}
	container := ""
	for _, name := range list {
		if strings.HasPrefix(name, containerName) {
			container = name
		}
	}
	if container == "" {
		dirPath, err := ioutil.TempDir(dir, containerName)
		if err != nil {
			return nil, err
		}
		container = filepath.Base(dirPath)
	}

	storageEntries := &storageEntries{
		index:     make(indexes),
		out:       outputPath,
		container: filepath.Join(dir, container),
	}

	db := &Db{
		out:     f,
		maxSize: sizeBytes,
		params:  storageEntries,
	}
	db.mergeHandler = NewMergeHandler(storageEntries, &db.mtx)
	db.writeHandler = NewWriteHandler(db.onWriteListener)

	go db.mergeHandler.StartLoop()
	go db.writeHandler.StartLoop()

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	list, err := listStorageEntries(db.params.container)
	if err != nil {
		return err
	}
	db.params.segmentCounter = len(list)

	err = db.execRecover(list)
	if db.params.segmentCounter > 1 {
		db.mergeHandler.Req <- true
		<-db.mergeHandler.Res
	}
	return err
}

func (db *Db) execRecover(dirEntries []string) error {
	list := append(dirEntries, db.params.out)
	for _, name := range list {
		if name != db.params.out {
			name = filepath.Join(db.params.container, name)
		}
		db.params.index[name] = make(hashIndex)
		var currentOffset int64
		input, err := os.Open(name)
		if err != nil {
			return err
		}
		defer input.Close()

		var buf [bufSize]byte
		in := bufio.NewReaderSize(input, bufSize)
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					continue
				}
			} else if err != nil {
				return err
			}
			size := binary.LittleEndian.Uint32(header)

			if size < bufSize {
				data = buf[:size]
			} else {
				data = make([]byte, size)
			}
			n, err = in.Read(data)

			if err == nil {
				if n != int(size) {
					return fmt.Errorf("corrupted file")
				}

				var e entry
				e.Decode(data)
				if err := compareHash(e.key, e.value, e.sum); err != nil {
					return err
				}

				db.params.index[name][e.key] = currentOffset
				currentOffset += int64(n)
			}
		}
		if name == db.params.out {
			db.outOffset = currentOffset
		}
	}
	return nil
}

func (db *Db) Close() error {
	if err := db.out.Close(); err != nil {
		return err
	}

	dir := filepath.Dir(db.params.out)
	list, err := listStorageEntries(dir)
	if err != nil {
		return err
	}
	out := filepath.Base(db.params.out)
	container := filepath.Base(db.params.container)
	for _, name := range list {
		if name != out && name != container {
			fullPath := filepath.Join(dir, name)
			if err := os.RemoveAll(fullPath); err != nil {
				return err
			}
		}
	}

	db.mergeHandler.Close()

	db.writeHandler.Close()
	return nil
}

func (db *Db) Get(key string) (string, error) {
  keys := getSortedKeys(db.params.index)
  db.mtx.Lock()
  // local copy for safe get operation
  dbIndex := db.params.index
  db.mtx.Unlock()
  for i := len(keys) - 1; i >= 0; i-- {
    fileName := keys[i]
    position, ok := dbIndex[fileName][key]
    if !ok {
      continue
    }

		file, err := os.Open(fileName)
		if err != nil {
			return "", err
		}
		defer file.Close()
		value, hashSum, err := searchValue(file, position)
		if err != nil {
			return "", err
		}
		if err := compareHash(key, value, hashSum); err != nil {
			return "", err
		} else {
			return value, nil
		}
	}
	return "", ErrNotFound
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
		sum:   getHashSum(key, value),
	}
	db.writeHandler.Req <- e
	err := <-db.writeHandler.Res
	return err
}

func (db *Db) onWriteListener() (closed bool) {
	e, more := <-db.writeHandler.Req
	if !more {
		closed = true
		return
	}
	encoded := e.Encode()

	f, err := os.Stat(db.params.out)
	if err != nil {
		db.writeHandler.Res <- err
		return
	}
	if f.Size()+int64(len(encoded)) >= db.maxSize {
		db.mtx.Lock()
		db.params.segmentCounter++
		db.mtx.Unlock()
		newName := fmt.Sprintf("%d-segment", db.params.segmentCounter)
		newPath := filepath.Join(db.params.container, newName)
		if err := db.out.Close(); err != nil {
			db.writeHandler.Res <- err
			return
		}
		if err := os.Rename(db.params.out, newPath); err != nil {
			db.writeHandler.Res <- err
			return
		}
		if file, err := os.Create(db.params.out); err != nil {
			db.writeHandler.Res <- err
			return
		} else {
			db.mtx.Lock()
			db.out = file
			db.outOffset = 0
			db.params.index[newPath] = db.params.index[db.params.out]
			db.params.index[db.params.out] = make(hashIndex)
			db.mtx.Unlock()
		}
	}
	putErr := error(nil)
	if db.params.segmentCounter > 1 {
		db.mergeHandler.Req <- true
		putErr = db.writeHash(e.key, encoded)
		<-db.mergeHandler.Res
	} else {
		putErr = db.writeHash(e.key, encoded)
	}
	db.writeHandler.Res <- putErr
	return
}

func (db *Db) writeHash(key string, encoded []byte) error {
	n, err := db.out.Write(encoded)
	if err == nil {
		db.mtx.Lock()
		db.params.index[db.params.out][key] = db.outOffset
		db.outOffset += int64(n)
		db.mtx.Unlock()
	}
	return err
}
