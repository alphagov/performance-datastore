package main

import (
	"flag"
	"github.com/alext/tablecloth"
	"github.com/go-martini/martini"
	"github.com/jabley/performance-datastore/pkg/handlers"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
)

func main() {
	if wd := os.Getenv("GOVUK_APP_ROOT"); wd != "" {
		tablecloth.WorkingDir = wd
	}

	var (
		port = flag.Int("port", 8080, "Port that the server should listen on")
	)

	flag.Parse()
	m := martini.Classic()
	m.Get("/_status", handlers.StatusHandler)
	m.Get("/_status/data-sets", handlers.DataSetStatusHandler)
	m.Get("/data/:data_group/:data_type", handlers.DataTypeHandler)
	m.Options("/data/:data_group/:data_type", handlers.DataTypeHandler)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go serve(":"+strconv.Itoa(*port), m, wg)
	wg.Wait()
}

func serve(addr string, m http.Handler, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Fatal(tablecloth.ListenAndServe(addr, m))
}
