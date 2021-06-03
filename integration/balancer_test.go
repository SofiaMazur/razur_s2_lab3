package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"
const serverAddress = "http://server1:8080/test?key="

var client = http.Client{
	Timeout: 3 * time.Second,
}

type OutData struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestBalancer(t *testing.T) {
	route := fmt.Sprintf("%s/api/v1/some-data?key=razur", baseAddress)
	time.Sleep(10 * time.Second)
	response, err := client.Get(route)
	assert.Nil(t, err)
	compare := response.Header.Get("lb-from")
	assert.Equal(t, compare, response.Header.Get("lb-from"))
	assert.Nil(t, err)
	assert.Equal(t, response.StatusCode, http.StatusOK)
	defer response.Body.Close()
	var incoming OutData
	err = json.NewDecoder(response.Body).Decode(&incoming)
	assert.Equal(t, incoming.Key, "razur")
	assert.Equal(t, incoming.Value, time.Now().Format("01-02-2006"))
	assert.Nil(t, err)
}
