package validation

import (
	"fmt"
	"testing"
	"time"
)

type expectation struct {
	t               *testing.T
	args            map[string][]string
	allowRawQueries bool
}

func TestDateTimeString(t *testing.T) {
	if !isValidDateTime("2012-06-03T13:26:00") {
		t.Errorf("WAT!")
	}
}

func TestBadlyFormattedStartAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"i am not a time"}
	expectError(expectation{t: t, args: args})
}

func TestWellFormattedStartAtIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	args["end_at"] = []string{"2000-02-09T00:02:02 +00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestMultipleStartAtArgsFail(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:02:02 +00:00", "2000-03-02T00:02:02 +00:00"}
	expectError(expectation{t: t, args: args})
}

func TestInvalidStartDateFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-14-28T00:02:02 +00:00"}
	expectError(expectation{t: t, args: args})
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
	expectError(expectation{t: t, args: args})
}

func TestWellFormattedEndAtIsAllowed(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-01-26T00:02:02 +00:00"}
	args["end_at"] = []string{"2000-02-02T00:02:02 +00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestFilterByQueryRequiresFieldAndName(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"bar"}
	expectError(expectation{t: t, args: args})
}

func TestWellFormattedFilterByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"foo:bar"}
	expectSuccess(expectation{t: t, args: args})
}

func TestAllFilterByArgsAreValidated(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"foo:bar", "baz"}
	expectError(expectation{t: t, args: args})
}

func TestFilterByFieldNameIsValidated(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"with-hyphen:bar"}
	expectError(expectation{t: t, args: args})
}

func TestFilterByFieldNameCannotLookLikeMongoThing(t *testing.T) {
	args := make(map[string][]string)
	args["filter_by"] = []string{"$foo:bar"}
	expectError(expectation{t: t, args: args})
}

func TestSortByAscendingIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	expectSuccess(expectation{t: t, args: args})
}

func TestSortByDescendingIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:descending"}
	expectSuccess(expectation{t: t, args: args})
}

func TestSortByAnythingElseFails(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:random"}
	expectError(expectation{t: t, args: args})
	args["sort_by"] = []string{"lulz"}
	expectError(expectation{t: t, args: args})
}

func TestSortByRequiresAValidFieldName(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"with-hyphen:ascending"}
	expectError(expectation{t: t, args: args})
}

func TestLimitShouldBeAPositiveInteger(t *testing.T) {
	args := make(map[string][]string)
	args["limit"] = []string{"not_a_number"}
	expectError(expectation{t: t, args: args})
	args["limit"] = []string{"-3"}
	expectError(expectation{t: t, args: args})
	args["limit"] = []string{"3"}
	expectSuccess(expectation{t: t, args: args})
}

func TestGroupByOnInternalNameFails(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"_internal_field"}
	expectError(expectation{t: t, args: args})
}

func TestGroupByOnInvalidFieldNameFails(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"with-hyphen"}
	expectError(expectation{t: t, args: args})
}

func TestSortByWithPeriodOnlyFails(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	args["period"] = []string{"week"}
	args["start_at"] = []string{"2012-11-12T00:00:00Z"}
	args["end_at"] = []string{"2012-12-03T00:00:00Z"}
	expectError(expectation{t: t, args: args})
}

func TestSortByWithPeriodAndGroupByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["sort_by"] = []string{"foo:ascending"}
	args["period"] = []string{"week"}
	args["group_by"] = []string{"foobar"}
	args["start_at"] = []string{"2012-11-12T00:00:00Z"}
	args["end_at"] = []string{"2012-12-03T00:00:00Z"}
	expectSuccess(expectation{t: t, args: args})
}

func TestCollectWithoutGroupByFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar"}
	expectError(expectation{t: t, args: args})
}

func TestCollectAndGroupByIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar"}
	args["group_by"] = []string{"foo"}
	expectSuccess(expectation{t: t, args: args})
}

func TestCollectIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"a_aAbBzZ_"}
	args["group_by"] = []string{"foo"}
	expectSuccess(expectation{t: t, args: args})
}

func TestCollectWithFunctionFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"something);while(1){myBadFunction()}"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestCollectWithAHyphenFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"with-hyphen"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestCollectWithAMongoThingFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"$foo"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestCollectOnSameFieldAsGroupByFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"foo"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestCollectOnInternalFieldFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"_foo"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestMultipleCollectIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar", "baz"}
	args["group_by"] = []string{"foo"}
	expectSuccess(expectation{t: t, args: args})
}

func TestCollectWithLaterInternalParameterFails(t *testing.T) {
	args := make(map[string][]string)
	args["collect"] = []string{"bar", "_baz"}
	args["group_by"] = []string{"foo"}
	expectError(expectation{t: t, args: args})
}

func TestCollectHasAWhitelistOfMethods(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"foo"}

	for _, method := range []string{"sum", "count", "set", "mean"} {
		args["collect"] = []string{fmt.Sprintf("field:%s", method)}
		expectSuccess(expectation{t: t, args: args})
	}
}

func TestCollectWithInvalidMethodFails(t *testing.T) {
	args := make(map[string][]string)
	args["group_by"] = []string{"foo"}
	args["collect"] = []string{"field:foobar"}
	expectError(expectation{t: t, args: args})
}

func TestDurationRequiresOtherParameters(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"3"}
	expectError(expectation{t: t, args: args})
}

func TestDurationMustBePositiveInteger(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"0"}
	args["period"] = []string{"day"}
	expectError(expectation{t: t, args: args})

	args["duration"] = []string{"3"}
	expectSuccess(expectation{t: t, args: args})

	args["duration"] = []string{"-3"}
	args["period"] = []string{"day"}
	expectError(expectation{t: t, args: args})
}

func TestDurationIsAValidNumber(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"not_a_number"}
	args["period"] = []string{"day"}
	expectError(expectation{t: t, args: args})
}

func TestPeriodAndDurationWithStartAtIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"3"}
	args["period"] = []string{"day"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestStartAtAloneFails(t *testing.T) {
	args := make(map[string][]string)
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestEndAtAloneFails(t *testing.T) {
	args := make(map[string][]string)
	args["end_at"] = []string{"2000-02-02T00:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestDurationWithStartAtAndEndAtFails(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"3"}
	args["period"] = []string{"day"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestPeriodHasALimitedVocabulary(t *testing.T) {
	args := make(map[string][]string)
	args["duration"] = []string{"3"}
	args["period"] = []string{"fortnight"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestPeriodWithStartAtAndEndAtIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"week"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args, allowRawQueries: true})
}

func TestNoRawQueriesWithPeriodWithStartAtAndEndAtOnWednesdayFails(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"week"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
	expectError(expectation{t: t, args: args, allowRawQueries: false})
}

func TestNoRawQueriesWithPeriodWithStartAtAndEndAtOnMondayIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"week"}
	args["start_at"] = []string{"2000-02-07T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-14T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args, allowRawQueries: false})
}

func TestNoRawQueriesMeansUseMidnight(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"day"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestNoRawQueriesForADayPeriodWithHourseInTheMiddleOfTheDayFails(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"day"}
	args["start_at"] = []string{"2000-02-02T12:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T13:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestNoRawQueriesForAnHourPeriodAllowsTimeInTheMiddleOfTheDay(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"hour"}
	args["start_at"] = []string{"2000-02-02T12:00:00+00:00"}
	args["end_at"] = []string{"2000-02-09T13:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestNoRawQueriesForADayPeriodLessThan7DaysFails(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"day"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-08T00:00:00+00:00"}
	expectError(expectation{t: t, args: args})
}

func TestNoRawQueriesForAnHourPeriodAreAllowed(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"hour"}
	args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
	args["end_at"] = []string{"2000-02-08T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args})
}

func TestNoRawQueriesWithMonthPeriodWithStartAtAndEndAtOnThirdFails(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"month"}
	args["start_at"] = []string{"2000-02-03T00:00:00+00:00"}
	args["end_at"] = []string{"2000-03-03T00:00:00+00:00"}
	expectError(expectation{t: t, args: args, allowRawQueries: false})
}

func TestNoRawQueriesWithMonthPeriodWithStartAtAndEndAtOnFirstIsOkay(t *testing.T) {
	args := make(map[string][]string)
	args["period"] = []string{"month"}
	args["start_at"] = []string{"2000-02-01T00:00:00+00:00"}
	args["end_at"] = []string{"2000-03-01T00:00:00+00:00"}
	expectSuccess(expectation{t: t, args: args, allowRawQueries: false})
}

func expectError(e expectation) {
	if ValidateRequestArgs(e.args, e.allowRawQueries) == nil {
		e.t.Errorf("%v should have failed", e.args)
	}
}

func expectSuccess(e expectation) {
	if err := ValidateRequestArgs(e.args, e.allowRawQueries); err != nil {
		e.t.Errorf("%v should have been okay but was %v", e.args, err)
	}
}
