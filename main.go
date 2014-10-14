package main

import (
	"github.com/alext/tablecloth"
	"github.com/go-martini/martini"
	"github.com/jabley/performance-datastore/pkg/config_api"
	"github.com/jabley/performance-datastore/pkg/handlers"
	"log"
	"net/http"
	"os"
	"sync"
)

func main() {
	if wd := os.Getenv("GOVUK_APP_ROOT"); wd != "" {
		tablecloth.WorkingDir = wd
	}

	var (
		port         = getEnvDefault("HTTP_PORT", "8080")
		databaseName = getEnvDefault("DBNAME", "backdrop")
		mongoURL     = getEnvDefault("MONGO_URL", "localhost")
		bearerToken  = getEnvDefault("BEARER_TOKEN", "EMPTY")
		configAPIURL = getEnvDefault("CONFIG_API_URL", "https://stagecraft.production.performance.service.gov.uk/")
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	handlers.ConfigAPIClient = config_api.NewClient(configAPIURL, bearerToken)
	handlers.DataSetStorage = handlers.NewMongoStorage(mongoURL, databaseName)

	m := registerRoutes()
	go serve(":"+port, m, wg)
	wg.Wait()
}

func serve(addr string, m http.Handler, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Fatal(tablecloth.ListenAndServe(addr, m))
}

func registerRoutes() http.Handler {
	m := martini.Classic()
	m.Get("/_status", handlers.StatusHandler)
	m.Get("/_status/data-sets", handlers.DataSetStatusHandler)
	m.Get("/data/:data_group/:data_type", handlers.DataTypeHandler)
	m.Options("/data/:data_group/:data_type", handlers.DataTypeHandler)
	m.Post("/data/:data_group/:data_type", handlers.CreateHandler)
	m.Put("/data/:data_group/:data_type", handlers.UpdateHandler)
	return m
}

func getEnvDefault(key string, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}

	return val
}
