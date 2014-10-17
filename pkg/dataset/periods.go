package dataset

import (
	"time"
)

type Period int

const (
	Hour Period = 1 << iota
	Day
	Week
	Month
	Quarter
	Year
)

var Periods = []Period{Hour, Day, Week, Month, Quarter, Year}

func (p Period) FieldName() string {
	var s string
	switch p {
	case Hour:
		s = "_hour_start_at"
	case Day:
		s = "_day_start_at"
	case Week:
		s = "_week_start_at"
	case Month:
		s = "_month_start_at"
	case Quarter:
		s = "_quarter_start_at"
	case Year:
		s = "_year_start_at"
	default:
		s = "Unknown Period"
	}
	return s
}

func (p Period) Value(t time.Time) (r time.Time) {
	switch p {
	case Hour:
		r = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case Day:
		r = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case Week:
		r = week(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
	case Month:
		r = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case Quarter:
		r = time.Date(t.Year(), quarterMonth(t.Month()), 1, 0, 0, 0, 0, t.Location())
	case Year:
		r = time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, t.Location())
	default:
		r = time.Now()
	}
	return
}

func quarterMonth(month time.Month) time.Month {
	switch {
	case month >= time.October:
		return time.October
	case month >= time.July:
		return time.July
	case month >= time.April:
		return time.April
	default:
		return time.January
	}
}

func week(t time.Time) (r time.Time) {
	switch t.Weekday() {
	case time.Sunday:
		r = t.AddDate(0, 0, -6)
	case time.Monday:
		r = t
	case time.Tuesday:
		r = t.AddDate(0, 0, -1)
	case time.Wednesday:
		r = t.AddDate(0, 0, -2)
	case time.Thursday:
		r = t.AddDate(0, 0, -3)
	case time.Friday:
		r = t.AddDate(0, 0, -4)
	case time.Saturday:
		r = t.AddDate(0, 0, -5)
	}
	return
}
