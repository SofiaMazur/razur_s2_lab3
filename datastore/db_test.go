package datastore

import (
	"os"
	"path/filepath"
	"testing"
)

// segment size for 6 records
const testSizeBytes = 260

var testValues = map[string]string {
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
}

func TestDb_Put(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	
	db, err := NewDb(dir, testSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	outFile, err := os.Open(filepath.Join(dir, outFileName))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for key, value := range testValues {
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
			foundValue, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if foundValue != value {
				t.Errorf("Bad value returned expected %s, got %s", value, foundValue)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for key, value := range testValues {
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 * 2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		
		db, err = NewDb(dir, testSizeBytes)
		if err != nil {
			t.Fatal(err)
		}

		for key, value := range testValues {
			foundValue, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
			if foundValue != value {
				t.Errorf("Bad value returned: expected %s, got %s", value, foundValue)
			}
		}
	})
}

func Test_Db_Segments(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-segments-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// test size for 3 records
	db, err := NewDb(dir, testSizeBytes / 2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	list, err := listStorageEntries(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Errorf("Invalid recovery")
	}
	containerName := filepath.Base(db.params.container)
	for _, name := range list {
		if name != containerName && name != outFileName {
			t.Errorf("Invalid out file name")
		}
	}
	containerList, err := listStorageEntries(db.params.container)
	if err != nil {
		t.Fatal(err)
	}
	if len(containerList) != 0 {
		t.Errorf("Invalid container entries")
	}

	for key, value := range testValues {
		for i := 0; i < 2; i++ {
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
		}
	}
	segmentList, err := listStorageEntries(db.params.container)
	if err != nil {
		t.Fatal(err)
	}
	if len(segmentList) != 1 || segmentList[0] != "1-segment" {
		t.Errorf("Invalid container entries")
	}

	for key, value := range testValues {
		for i := 0; i < 2; i++ {
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
		}
	}
	newContainer := filepath.Base(db.params.container)
	if newContainer == containerName {
		t.Errorf("Segmentation failed")
	}
}

func Test_Db_Records(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-records-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// test size for 3 records
	db, err := NewDb(dir, testSizeBytes / 2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key1 := "key1"
	value1 := testValues[key1]
	if err := db.Put(key1, value1); err != nil {
		t.Errorf("Cannot put %s: %s", key1, err)
	}
	if found, err := db.Get(key1); err != nil {
		t.Errorf("Cannot get %s: %s", key1, err)
	} else if found != value1 {
		t.Errorf("Bad value returned: expected %s, got %s", value1, found)
	}

	newVal := "new value"
	if err := db.Put(key1, newVal); err != nil {
		t.Errorf("Cannot put %s: %s", key1, err)
	}
	if found, err := db.Get(key1); err != nil {
		t.Errorf("Cannot get %s: %s", key1, err)
	} else if found != newVal {
		t.Errorf("Bad value returned: expected %s, got %s", value1, found)
	}

	key4 := "key4"
	value4 := "value4"
	if err := db.Put(key4, value4); err != nil {
		t.Errorf("Cannot put %s: %s", key4, err)
	}
	if found, err := db.Get(key1); err != nil {
		t.Errorf("Cannot get %s: %s", key1, err)
	} else if found != newVal {
		t.Errorf("Bad value returned: expected %s, got %s", value1, found)
	}

	for key, value := range testValues {
		for i := 0; i < 2; i++ {
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}
		}
	}

	// After creating a segment
	if found, err := db.Get(key4); err != nil {
		t.Errorf("Cannot get %s: %s", key4, err)
	} else if found != value4 {
		t.Errorf("Bad value returned: expected %s, got %s", value4, found)
	}
}

func Test_Db_Hash(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-hash-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, testSizeBytes / 2)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key1 := "key1"
	value1 := testValues[key1]
	if err := db.Put(key1, value1); err != nil {
		t.Errorf("Cannot put %s: %s", key1, err)
	}
	if found, err := db.Get(key1); err != nil {
		t.Errorf("Cannot get %s: %s", key1, err)
	} else if found != value1 {
		t.Errorf("Bad value returned: expected %s, got %s", value1, found)
	}

	e := entry{
		key:   key1,
		value: value1,
		sum:	 getHashSum(key1, value1 + "_test"),
	}
	db.writeHash(e.key, e.Encode())
	if _, err := db.Get(key1); err != ErrHashSums {
		t.Log(err)
		t.Errorf("Unexpected hash sum behaviour")
	}
}
