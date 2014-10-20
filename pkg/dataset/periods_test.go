package dataset_test

import (
	. "github.com/jabley/performance-datastore/pkg/dataset"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Periods", func() {
	Describe("FieldNames", func() {
		It("should have appropriate FieldNames", func() {
			Expect(Hour.FieldName()).Should(Equal("_hour_start_at"))
			Expect(Day.FieldName()).Should(Equal("_day_start_at"))
			Expect(Week.FieldName()).Should(Equal("_week_start_at"))
			Expect(Month.FieldName()).Should(Equal("_month_start_at"))
			Expect(Quarter.FieldName()).Should(Equal("_quarter_start_at"))
			Expect(Year.FieldName()).Should(Equal("_year_start_at"))
		})
	})
	Describe("Values", func() {
		var currentTime time.Time

		BeforeEach(func() {
			currentTime = time.Date(2006, time.January, 2, 22, 04, 05, 0, time.UTC)
		})

		It("Hour value should be start of hour", func() {
			Expect(Hour.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 22, 0, 0, 0, time.UTC)))
		})

		It("Day value should be midnight UTC of the given day", func() {
			Expect(Day.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
		})

		Describe("Week", func() {
			oneDay := time.Hour * 24

			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 2))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 3))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 4))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 5))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 6))).Should(Equal(time.Date(2006, time.January, 2, 0, 0, 0, 0, time.UTC)))
			})
			It("Week value should be midnight UTC of the given day", func() {
				Expect(Week.Value(currentTime.Add(oneDay * 7))).Should(Equal(time.Date(2006, time.January, 9, 0, 0, 0, 0, time.UTC)))
			})
		})

		It("Month value should be midnight UTC of the given day", func() {
			Expect(Month.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
		})

		Describe("Quarter", func() {
			Context("January quarter", func() {
				It("Quarter for January", func() {
					Expect(Quarter.Value(time.Date(2006, time.January, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for February", func() {
					Expect(Quarter.Value(time.Date(2006, time.February, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for March", func() {
					Expect(Quarter.Value(time.Date(2006, time.March, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("April Quarter", func() {
				It("Quarter for April", func() {
					Expect(Quarter.Value(time.Date(2006, time.April, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for May", func() {
					Expect(Quarter.Value(time.Date(2006, time.May, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for June", func() {
					Expect(Quarter.Value(time.Date(2006, time.June, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.April, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("July Quarter", func() {
				It("Quarter for July", func() {
					Expect(Quarter.Value(time.Date(2006, time.July, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for August", func() {
					Expect(Quarter.Value(time.Date(2006, time.August, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for September", func() {
					Expect(Quarter.Value(time.Date(2006, time.September, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
			Context("October Quarter", func() {
				It("Quarter for October", func() {
					Expect(Quarter.Value(time.Date(2006, time.October, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for November", func() {
					Expect(Quarter.Value(time.Date(2006, time.November, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
				It("Quarter for December", func() {
					Expect(Quarter.Value(time.Date(2006, time.December, 2, 22, 04, 05, 0, time.UTC))).Should(Equal(time.Date(2006, time.October, 1, 0, 0, 0, 0, time.UTC)))
				})
			})
		})

		It("Year value should be midnight UTC of the given day", func() {
			Expect(Year.Value(currentTime)).Should(Equal(time.Date(2006, time.January, 1, 0, 0, 0, 0, time.UTC)))
		})
	})
})
