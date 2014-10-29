package handlers

import (
	"github.com/alphagov/performance-datastore/pkg/dataset"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

var (
	mgoSession *mgo.Session
)

// MongoDataSetStorage is an implementation of DataSetStorage.
type MongoDataSetStorage struct {
	URL          string
	DatabaseName string
}

func getMgoSession(URL string) *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.DialWithTimeout(URL, 5*time.Second)
		if err != nil {
			panic(err)
		}
		// Set timeout to suitably small value by default.
		mgoSession.SetSyncTimeout(5 * time.Second)
	}
	return mgoSession.Copy()
}

// NewMongoStorage creates a new MongoDataSetStorage.
func NewMongoStorage(URL string, databaseName string) dataset.DataSetStorage {
	return &MongoDataSetStorage{URL, databaseName}
}

// Create creates the named DataSet, returning an error if there was a problem.
func (m *MongoDataSetStorage) Create(name string, cappedSize int64) error {
	session := getMgoSession(m.URL)
	defer session.Close()

	info := &mgo.CollectionInfo{}
	if cappedSize != 0 {
		info.MaxBytes = int(cappedSize)
		info.Capped = true
	}

	// TODO would probably like some indices on the collection? _timestamp, for instance?
	return session.DB(m.DatabaseName).C(m.DatabaseName).Create(info)
}

// Exists returns true if the named DataSet exists, otherwise false.
func (m *MongoDataSetStorage) Exists(name string) bool {
	session := getMgoSession(m.URL)
	defer session.Close()

	names, err := session.DB(m.DatabaseName).CollectionNames()

	if err != nil {
		panic(err)
	}

	for _, n := range names {
		if n == name {
			return true
		}
	}

	return false
}

// Alive returns true if we can talk to a mongodb instance, otherwise false
func (m *MongoDataSetStorage) Alive() bool {
	session := getMgoSession(m.URL)
	defer session.Close()

	session.SetMode(mgo.Eventual, true)

	return len(session.LiveServers()) > 0
}

// Empty empties the named DataSet.
func (m *MongoDataSetStorage) Empty(name string) error {
	session := getMgoSession(m.URL)
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	coll := session.DB(m.DatabaseName).C(name)
	_, err := coll.RemoveAll(nil)
	return err
}

// LastUpdated returns the time that the named DataSet was last updated, or nil if it never has been updated.
func (m *MongoDataSetStorage) LastUpdated(name string) (t *time.Time) {
	session := getMgoSession(m.URL)
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	var lastUpdated bson.M

	coll := session.DB(m.DatabaseName).C(name)
	err := coll.Find(nil).Sort("-_updated_at").One(&lastUpdated)

	if err != nil {
		panic(err)
	}

	t = nil

	value, isTime := lastUpdated["_updated_at"].(time.Time)

	if isTime {
		t = &value
	}

	return
}

// SaveRecord saves the given JSON record in the named DataSet, returning an error if there was a problem.
func (m *MongoDataSetStorage) SaveRecord(name string, record map[string]interface{}) error {
	session := getMgoSession(m.URL)
	defer session.Close()
	coll := session.DB(m.DatabaseName).C(name)

	return coll.Insert(record)
}
