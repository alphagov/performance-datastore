package dataset

import (
	"testing"
)

func TestPublishedDefaultValue(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	if dataSet.IsPublished() {
		t.Error("Default value for isPublished should be false")
	}
}

func TestPublishedFlagIsRead(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["published"] = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.IsPublished() {
		t.Error("Published field was not read")
	}
}

func TestPublishedFlagWithNonBoolValueIsFalsy(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["published"] = "not-boolean"
	dataSet := DataSet{nil, metaData}
	if dataSet.IsPublished() {
		t.Error("Default value for non-boolean published field should be false")
	}
}

func TestQueryableDefaultValue(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	if dataSet.IsQueryable() {
		t.Error("Default value for isQueryable should be false")
	}
}

func TestQueryableFlagIsRead(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["queryable"] = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.IsQueryable() {
		t.Error("Queryable field was not read")
	}
}

func TestQueryableFlagWithNonBoolValueIsFalsy(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["queryable"] = "not-boolean"
	dataSet := DataSet{nil, metaData}
	if dataSet.IsQueryable() {
		t.Error("Default value for non-boolean queryable field should be false")
	}
}
