package handlers

import (
	"github.com/jabley/performance-datastore/pkg/dataset"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

var (
	mgoSession *mgo.Session
)

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

func NewMongoStorage(URL string, databaseName string) dataset.DataSetStorage {
	return &MongoDataSetStorage{URL, databaseName}
}

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

func (m *MongoDataSetStorage) Alive() bool {
	session := getMgoSession(m.URL)
	defer session.Close()

	session.SetMode(mgo.Eventual, true)

	return len(session.LiveServers()) > 0
}

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
