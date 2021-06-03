package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/razur_s2_lab3/httptools"
	"github.com/razur_s2_lab3/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

type InData struct {
	Value string
}

type OutData struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

const baseUrl string = "http://db:8091"

func main() {
	client := http.DefaultClient
	h := new(http.ServeMux)
	var team InData
	team.Value = time.Now().Format("01-02-2006")
	bytedResponse, err := json.Marshal(&team)
	if err != nil {
		panic(err)
	}

	response, err := client.Post(baseUrl+"/db/razur", "application/json", bytes.NewBuffer(bytedResponse))
	if err != nil {
		panic(err)
	}
	if response.StatusCode != http.StatusOK {
		panic(response.Status)
	}

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		keys, ok := r.URL.Query()["key"]

		if !ok || len(keys[0]) < 1 {
			http.Error(rw, "422: Url Param 'key' is missing", http.StatusUnprocessableEntity)
			return
		}

		key := keys[0]

		url := fmt.Sprintf(baseUrl+"/db/%s", key)
		response, err := client.Get(url)
		if err != nil {
			panic(err)
		}
		if response.StatusCode != http.StatusOK {
			http.Error(rw, "{}", response.StatusCode)
			return
		}
		defer response.Body.Close()
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}
		var incoming InData
		err = json.Unmarshal(body, &incoming)
		if err != nil {
			panic(err)
		}
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		var outgoing OutData
		outgoing.Key = key
		outgoing.Value = incoming.Value
		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(outgoing)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
