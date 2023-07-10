package test_api

import (
	"csv-handler/api"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestHandleGetData(t *testing.T) {
	// Load the configuration file
	viper.SetConfigFile("./../config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Failed to read configuration file: %v", err)
	}
	// Create a new request with query parameters
	req, err := http.NewRequest("GET", "/data?limit=1&offset=5", nil)
	assert.NoError(t, err)

	// Create a response recorder to capture the response
	res := httptest.NewRecorder()

	// Call the handler function
	api.HandleGetData(res, req)

	// Check the response status code
	assert.Equal(t, http.StatusOK, res.Code)

	// Check the response body (optional)
	// ...

	// Add more test cases as needed
}
