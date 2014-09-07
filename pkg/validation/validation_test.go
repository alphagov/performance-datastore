package validation

import (
	"testing"
	"time"
)

func TestDateTimeString(t *testing.T) {
	if !isValidDateTime("2012-06-03T13:26:00") {
		t.Errorf("WAT!")
	}
}

func TestBadlyFormattedStartAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"i am not a time"}
	expectError(t, args)
}

func TestWellFormattedStartAtIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	expectSuccess(t, args)
}

func TestMultipleStartAtArgsFail(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00", "2000-03-02T00:02:02 +00:00"}
	expectError(t, args)
}

func TestDaftStartDateFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-14-28T00:02:02 +00:00"}
	expectError(t, args)
}

func TestDateParsing(t *testing.T) {
	v, err := time.Parse(time.RFC3339, "2000-14-28T00:02:02 +00:00")

	if err == nil {
		t.Errorf("time was parsed as %v", v)
	}
}

func TestBadlyFormattedEndAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["end_at"] = []string{"i am not a time"}
	expectError(t, args)
}

func TestWellFormattedEndAtIsAllowed(t *testing.T) {
	args := make(map[string][]string)
	args["end_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	expectSuccess(t, args)
}

func TestFilterByQueryRequiresFieldAndName(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"bar"}
	expectError(t, args)
}

func TestWellFormattedFilterByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"foo:bar"}
	expectSuccess(t, args)
}

func TestAllFilterByArgsAreValidated(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"foo:bar", "baz"}
	expectError(t, args)
}

func TestFilterByFieldNameIsValidated(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"with-hyphen:bar"}
	expectError(t, args)
}

func TestFilterByFieldNameCannotLookLikeMongoThing(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"$foo:bar"}
	expectError(t, args)
}

func TestSortByAscendingIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	expectSuccess(t, args)
}

func TestSortByDescendingIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:descending"}
	expectSuccess(t, args)
}

func TestSortByAnythingElseFails(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:random"}
	expectError(t, args)
	args["sort_by"] = []string{"lulz"}
	expectError(t, args)
}

func TestSortByRequiresAValidFieldName(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"with-hypthen:ascending"}
	expectError(t, args)
}

func TestLimitShouldBeAPositiveInteger(t *testing.T) {
	args := make(map[string][]string)
	args["limit"] = []string{"not_a_number"}
	expectError(t, args)
	args["limit"] = []string{"-3"}
	expectError(t, args)
	args["limit"] = []string{"3"}
	expectSuccess(t, args)
}

func TestGroupByOnInternalNameFails(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"_internal_field"}
	expectError(t, args)
}

func TestGroupByOnInvalidFieldNameFails(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"with-hyphen"}
	expectError(t, args)
}

func TestSortByWithPeriodOnlyFails(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	args["period"] = []string{"week"}
	args["start_at"] = []string{"2012-11-12T00:00:00Z"}
	args["end_at"] = []string{"2012-12-03T00:00:00Z"}
	expectError(t, args)
}

func TestSortByWithPeriodAndGroupByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	args["period"] = []string{"week"}
	args["group_by"] = []string{"foobar"}
	args["start_at"] = []string{"2012-11-12T00:00:00Z"}
	args["end_at"] = []string{"2012-12-03T00:00:00Z"}
	expectSuccess(t, args)
}

func TestCollectWithoutGroupByFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar"}
	expectError(t, args)
}

func TestCollectAndGroupByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar"}
	args["group_by"] = []string{"foo"}
	expectSuccess(t, args)
}

func TestCollectIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"a_aAbBzZ_"}
	args["group_by"] = []string{"foo"}
	expectSuccess(t, args)
}

func TestCollectWithFunctionFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"something);while(1){myBadFunction()}"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func TestCollectWithAHyphenFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"with-hyphen"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func TestCollectWithAMongoThingFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"$foo"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func TestCollectOnSameFieldAsGroupByFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"foo"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func TestCollectOnInternalFieldFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"_foo"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func TestMultipleCollectIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar", "baz"}
	args["group_by"] = []string{"foo"}
	expectSuccess(t, args)
}

func TestCollectWithLaterInternalParameterFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar", "_baz"}
	args["group_by"] = []string{"foo"}
	expectError(t, args)
}

func expectError(t *testing.T, args map[string][]string) {
	if ValidateRequestArgs(args, false) == nil {
		t.Errorf("%v should have failed", args)
	}
}

func expectSuccess(t *testing.T, args map[string][]string) {
	if err := ValidateRequestArgs(args, false); err != nil {
		t.Errorf("%v should have been okay but was %v", args, err)
	}
}
