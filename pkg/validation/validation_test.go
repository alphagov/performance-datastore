package validation

import (
	"fmt"

	"time"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("Testing with Ginkgo", func() {
	It("date time string", func() {

		if !isValidDateTime("2012-06-03T13:26:00") {
			GinkgoT().Errorf("WAT!")
		}
	})
	It("badly formatted start at fails", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"i am not a time"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("well formatted start at is okay", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"2000-02-02T00:02:02 +00:00"}
		args["end_at"] = []string{"2000-02-09T00:02:02 +00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("multiple start at args fail", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"2000-02-02T00:02:02 +00:00", "2000-03-02T00:02:02 +00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("invalid start date fails", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"2000-14-28T00:02:02 +00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("date parsing", func() {

		v, err := time.Parse(time.RFC3339, "2000-14-28T00:02:02 +00:00")

		if err == nil {
			GinkgoT().Errorf("time was parsed as %v", v)
		}
	})
	It("badly formatted end at fails", func() {

		args := make(map[string][]string)
		args["end_at"] = []string{"i am not a time"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("well formatted end at is allowed", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"2000-01-26T00:02:02 +00:00"}
		args["end_at"] = []string{"2000-02-02T00:02:02 +00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("filter by query requires field and name", func() {

		args := make(map[string][]string)
		args["filter_by"] = []string{"bar"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("well formatted filter by is okay", func() {

		args := make(map[string][]string)
		args["filter_by"] = []string{"foo:bar"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("all filter by args are validated", func() {

		args := make(map[string][]string)
		args["filter_by"] = []string{"foo:bar", "baz"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("filter by field name is validated", func() {

		args := make(map[string][]string)
		args["filter_by"] = []string{"with-hyphen:bar"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("filter by field name cannot look like mongo thing", func() {

		args := make(map[string][]string)
		args["filter_by"] = []string{"$foo:bar"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("sort by ascending is okay", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"foo:ascending"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("sort by descending is okay", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"foo:descending"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("sort by anything else fails", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"foo:random"}
		expectError(expectation{t: GinkgoT(), args: args})
		args["sort_by"] = []string{"lulz"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("sort by requires a valid field name", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"with-hyphen:ascending"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("limit should be a positive integer", func() {

		args := make(map[string][]string)
		args["limit"] = []string{"not_a_number"}
		expectError(expectation{t: GinkgoT(), args: args})
		args["limit"] = []string{"-3"}
		expectError(expectation{t: GinkgoT(), args: args})
		args["limit"] = []string{"3"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("group by on internal name fails", func() {

		args := make(map[string][]string)
		args["group_by"] = []string{"_internal_field"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("group by on invalid field name fails", func() {

		args := make(map[string][]string)
		args["group_by"] = []string{"with-hyphen"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("sort by with period only fails", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"foo:ascending"}
		args["period"] = []string{"week"}
		args["start_at"] = []string{"2012-11-12T00:00:00Z"}
		args["end_at"] = []string{"2012-12-03T00:00:00Z"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("sort by with period and group by is okay", func() {

		args := make(map[string][]string)
		args["sort_by"] = []string{"foo:ascending"}
		args["period"] = []string{"week"}
		args["group_by"] = []string{"foobar"}
		args["start_at"] = []string{"2012-11-12T00:00:00Z"}
		args["end_at"] = []string{"2012-12-03T00:00:00Z"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("collect without group by fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"bar"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect and group by is okay", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"bar"}
		args["group_by"] = []string{"foo"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("collect is okay", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"a_aAbBzZ_"}
		args["group_by"] = []string{"foo"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("collect with function fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"something);while(1){myBadFunction()}"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect with a hyphen fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"with-hyphen"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect with a mongo thing fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"$foo"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect on same field as group by fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"foo"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect on internal field fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"_foo"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("multiple collect is okay", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"bar", "baz"}
		args["group_by"] = []string{"foo"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("collect with later internal parameter fails", func() {

		args := make(map[string][]string)
		args["collect"] = []string{"bar", "_baz"}
		args["group_by"] = []string{"foo"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("collect has a whitelist of methods", func() {

		args := make(map[string][]string)
		args["group_by"] = []string{"foo"}

		for _, method := range []string{"sum", "count", "set", "mean"} {
			args["collect"] = []string{fmt.Sprintf("field:%s", method)}
			expectSuccess(expectation{t: GinkgoT(), args: args})
		}
	})
	It("collect with invalid method fails", func() {

		args := make(map[string][]string)
		args["group_by"] = []string{"foo"}
		args["collect"] = []string{"field:foobar"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("duration requires other parameters", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"3"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("duration must be positive integer", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"0"}
		args["period"] = []string{"day"}
		expectError(expectation{t: GinkgoT(), args: args})

		args["duration"] = []string{"3"}
		expectSuccess(expectation{t: GinkgoT(), args: args})

		args["duration"] = []string{"-3"}
		args["period"] = []string{"day"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("duration is a valid number", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"not_a_number"}
		args["period"] = []string{"day"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("period and duration with start at is okay", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"3"}
		args["period"] = []string{"day"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("start at alone fails", func() {

		args := make(map[string][]string)
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("end at alone fails", func() {

		args := make(map[string][]string)
		args["end_at"] = []string{"2000-02-02T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("duration with start at and end at fails", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"3"}
		args["period"] = []string{"day"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("period has a limited vocabulary", func() {

		args := make(map[string][]string)
		args["duration"] = []string{"3"}
		args["period"] = []string{"fortnight"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("period with start at and end at is okay", func() {

		args := make(map[string][]string)
		args["period"] = []string{"week"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args, allowRawQueries: true})
	})
	It("no raw queries with period with start at and end at on wednesday fails", func() {

		args := make(map[string][]string)
		args["period"] = []string{"week"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args, allowRawQueries: false})
	})
	It("no raw queries with period with start at and end at on monday is okay", func() {

		args := make(map[string][]string)
		args["period"] = []string{"week"}
		args["start_at"] = []string{"2000-02-07T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-14T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args, allowRawQueries: false})
	})
	It("no raw queries means use midnight", func() {

		args := make(map[string][]string)
		args["period"] = []string{"day"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("no raw queries for a day period with hourse in the middle of the day fails", func() {

		args := make(map[string][]string)
		args["period"] = []string{"day"}
		args["start_at"] = []string{"2000-02-02T12:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T13:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("no raw queries for an hour period allows time in the middle of the day", func() {

		args := make(map[string][]string)
		args["period"] = []string{"hour"}
		args["start_at"] = []string{"2000-02-02T12:00:00+00:00"}
		args["end_at"] = []string{"2000-02-09T13:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("no raw queries for a day period less than7 days fails", func() {

		args := make(map[string][]string)
		args["period"] = []string{"day"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-08T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args})
	})
	It("no raw queries for an hour period are allowed", func() {

		args := make(map[string][]string)
		args["period"] = []string{"hour"}
		args["start_at"] = []string{"2000-02-02T00:00:00+00:00"}
		args["end_at"] = []string{"2000-02-08T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args})
	})
	It("no raw queries with month period with start at and end at on third fails", func() {

		args := make(map[string][]string)
		args["period"] = []string{"month"}
		args["start_at"] = []string{"2000-02-03T00:00:00+00:00"}
		args["end_at"] = []string{"2000-03-03T00:00:00+00:00"}
		expectError(expectation{t: GinkgoT(), args: args, allowRawQueries: false})
	})
	It("no raw queries with month period with start at and end at on first is okay", func() {

		args := make(map[string][]string)
		args["period"] = []string{"month"}
		args["start_at"] = []string{"2000-02-01T00:00:00+00:00"}
		args["end_at"] = []string{"2000-03-01T00:00:00+00:00"}
		expectSuccess(expectation{t: GinkgoT(), args: args, allowRawQueries: false})
	})
})

type expectation struct {
	t               GinkgoTInterface
	args            map[string][]string
	allowRawQueries bool
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
