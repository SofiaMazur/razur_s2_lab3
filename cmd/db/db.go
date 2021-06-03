package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/razur_s2_lab3/datastore"
	"github.com/razur_s2_lab3/signal"
)

type InData struct {
	Value string	`json:"value"`
}

type OutData struct {
	Key   string	`json:"key"`
	Value string	`json:"value"`
}

const port string = "8091"
const path string = "./out/storage/"

func dbHandler(db *datastore.Db) func (http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/db/"):]
		var c InData
		if r.Method == "POST" {
			defer r.Body.Close()
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "{}", http.StatusInternalServerError)
				return
			}
			if err = json.Unmarshal(body, &c); err != nil {
				http.Error(w, "{}", http.StatusInternalServerError)
				return
			}
			if err = db.Put(key, c.Value); err != nil {
				http.Error(w, "{}", http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusOK)
			}
			return
		}

		if r.Method == "GET" {
			value, err := db.Get(key)
			if err != nil {
				if err == datastore.ErrNotFound || err == datastore.ErrHashSums {
					http.Error(w, "{}", http.StatusNotFound)
				} else {
					http.Error(w, "{}", http.StatusInternalServerError)
				}
				return
			}
			c.Value = value
			if res, err := json.Marshal(&c); err != nil {
				http.Error(w, "{}", http.StatusInternalServerError)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(res)
			}
		}
	}
}

func main() {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		e := os.MkdirAll(path, os.ModePerm)
		if e != nil {
			panic(e)
		}
	}
	sizeBytes := datastore.MaxFileSizeMb * 1024 * 1024
	db, err := datastore.NewDb(path, int64(sizeBytes))
	if err != nil {
		panic(err)
	} else {
		defer db.Close()
	}

	
	http.HandleFunc("/db/", dbHandler(db))
	log.Printf("Starting server on " + port + " port...")
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	} else {
		signal.WaitForTerminationSignal()
	}
}
