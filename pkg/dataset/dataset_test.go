package dataset

import (
	"github.com/jabley/performance-datastore/pkg/config_api"
	"testing"
)

func TestPublishedDefaultValue(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	dataSet := DataSet{nil, metaData}
	if dataSet.IsPublished() {
		t.Error("Default value for isPublished should be false")
	}
}

func TestPublishedFlagIsRead(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	metaData.Published = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.IsPublished() {
		t.Error("Published field was not read")
	}
}

func TestQueryableDefaultValue(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	dataSet := DataSet{nil, metaData}
	if dataSet.IsQueryable() {
		t.Error("Default value for isQueryable should be false")
	}
}

func TestQueryableFlagIsRead(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	metaData.Queryable = true
	dataSet := DataSet{nil, metaData}
	if !dataSet.IsQueryable() {
		t.Error("Queryable field was not read")
	}
}

func TestCappedSize(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	metaData.CappedSize = 12345
	dataSet := DataSet{nil, metaData}
	cappedSize := dataSet.CappedSize()
	if int64(12345) != cappedSize {
		t.Errorf("Expected <%v> but was <%v>", 12345, cappedSize)
	}
}

func TestNoCappedSizeIsNil(t *testing.T) {
	metaData := config_api.DataSetMetaData{}
	dataSet := DataSet{nil, metaData}
	cappedSize := dataSet.CappedSize()
	if cappedSize != 0 {
		t.Errorf("Expected <%v> but was <%v>", 0, cappedSize)
	}
}
