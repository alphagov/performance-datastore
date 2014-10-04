package handlers

import (
	"labix.org/v2/mgo"
	"time"
)

var (
	mgoSession      *mgo.Session
	mgoDatabaseName = "backdrop"
	mgoURL          = "localhost"
)

func getMgoSession() *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.DialWithTimeout(mgoURL, 5*time.Second)
		if err != nil {
			panic(err)
		}
		// Set timeout to suitably small value by default.
		mgoSession.SetSyncTimeout(5 * time.Second)
	}
	return mgoSession.Copy()
}
