package routes

import (
	"csv-handler/api"

	"github.com/gorilla/mux"
)

// SetupRoutes sets up the API routes with a global prefix
func SetupRoutes(router *mux.Router, prefix string) {
	apiRouter := router.PathPrefix(prefix).Subrouter()

	// Register the API routes
	apiRouter.HandleFunc("/data", api.HandleGetData).Methods("GET")
	apiRouter.HandleFunc("/upload", api.HandleFileUpload).Methods("POST")

}
