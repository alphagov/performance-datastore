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

func TestCappedSize(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	metaData["capped_size"] = 12345
	dataSet := DataSet{nil, metaData}
	cappedSize := dataSet.CappedSize()
	if cappedSize == nil {
		t.Fatalf("CappedSize should not be nil")
	}
	if int(12345) != *cappedSize {
		t.Error("CappedSize should not be nil")
	}
}

func TestNoCappedSizeIsNil(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	cappedSize := dataSet.CappedSize()
	if cappedSize != nil {
		t.Fatalf("CappedSize should be nil if not explicitly set")
	}
}

func TestNonIntCappedSizeIsTreatedAsNil(t *testing.T) {
	var metaData DataSetMetaData
	metaData = make(map[string]interface{})
	dataSet := DataSet{nil, metaData}
	metaData["capped_size"] = "not-an-int"
	cappedSize := dataSet.CappedSize()
	if cappedSize != nil {
		t.Fatalf("CappedSize should be nil if not explicitly set")
	}
}
