package main

import (
	"encoding/json"
	"flag"
	// "fmt"
	"log"
	"net/http"
	"strconv"
)

type statusResponse struct {
	// Field names should be public, so that encoding/json can see them
	Status  string `json:"status"`
	Message string `json:"message"`
}

func main() {
	var (
		port = flag.Int("port", 8080, "Port that the server should listen on")
	)

	flag.Parse()
	http.Handle("/_status", http.HandlerFunc(statusHandler))

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "none")
	status := statusResponse{
		Status:  "ok",
		Message: "database seems fine",
	}
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(status); err != nil {
		panic(err)
	}

}
