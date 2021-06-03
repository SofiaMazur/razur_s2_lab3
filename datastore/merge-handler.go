package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

func NewMergeHandler(storageParams *storageEntries, mtx *sync.Mutex) *MergeHandler {
	return &MergeHandler{
		Req:           make(chan bool),
		Res:           make(chan error),
		closed:        make(chan bool),
		storageParams: storageParams,
		mtx:           mtx,
	}
}

type MergeHandler struct {
	Req           chan bool
	Res           chan error
	storageParams *storageEntries
	mtx           *sync.Mutex
	closed        chan bool
}

func (mh *MergeHandler) StartLoop() {
	go func() {
		for {
			closed := mh.merge()
			if closed {
				break
			}
		}
		mh.closed <- true
	}()
}

func (mh *MergeHandler) Close() {
	close(mh.Req)
	close(mh.Res)
	<-mh.closed
}

func (mh *MergeHandler) merge() (closed bool) {
	_, more := <-mh.Req
	if !more {
		closed = true
		return
	}
	keys := getSortedKeys(mh.storageParams.index)

	dir := filepath.Dir(mh.storageParams.out)
	container, err := ioutil.TempDir(dir, containerName)
	if err != nil {
		mh.Res <- err
		return
	}

	mh.mtx.Lock()
	mh.storageParams.segmentCounter = 1
	mh.mtx.Unlock()
	segmentName := fmt.Sprintf("%d-segment", mh.storageParams.segmentCounter)
	segmentPath := filepath.Join(container, segmentName)
	segment, err := os.Create(segmentPath)
	if err != nil {
		mh.Res <- err
		return
	}
	defer segment.Close()

	var segmentOffset int64
	segmentHash := make(hashIndex)
	for i := len(keys) - 1; i >= 0; i-- {
		fileName := keys[i]
		if fileName == mh.storageParams.out {
			continue
		}
		mergable, err := os.Open(fileName)
		if err != nil {
			mergable.Close()
			mh.Res <- err
			return
		}
		hash, ok := mh.storageParams.index[fileName]
		if !ok {
			mergable.Close()
			mh.Res <- err
			return
		}

		for key, offset := range hash {
			if _, found := segmentHash[key]; !found {
				if value, _, err := searchValue(mergable, offset); err != nil {
					mergable.Close()
					mh.Res <- err
					return
				} else {
					e := entry{
						key:   key,
						value: value,
						sum:   getHashSum(key, value),
					}
					encoded := e.Encode()
					if n, err := segment.Write(encoded); err != nil {
						mergable.Close()
						mh.Res <- err
						return
					} else {
						segmentHash[key] = segmentOffset
						segmentOffset += int64(n)
					}
				}
			}
		}
		mergable.Close()
	}

	if err := os.RemoveAll(mh.storageParams.container); err != nil {
		mh.Res <- err
		return
	}
	mh.mtx.Lock()
	mh.storageParams.container = container
	for _, key := range keys {
		if key != mh.storageParams.out {
			// safely deleting old hash indices
			delete(mh.storageParams.index, key)
		}
	}
	mh.storageParams.index[segmentPath] = segmentHash
	mh.mtx.Unlock()
	mh.Res <- nil
	return
}
