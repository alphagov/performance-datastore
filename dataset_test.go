package main

import (
	"testing"
)

func TestPublishedDefaultValue(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	if dataSet.isPublished() {
		t.Error("Default value for isPublished should be false")
	}
}

func TestPublishedFlagIsRead(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["published"] = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.isPublished() {
		t.Error("Published field was not read")
	}
}

func TestQueryableDefaultValue(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	if dataSet.isQueryable() {
		t.Error("Default value for isQueryable should be false")
	}
}

func TestQueryableFlagIsRead(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["queryable"] = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.isQueryable() {
		t.Error("Queryable field was not read")
	}
}
