package validation

type Validator interface {
	Validate(args map[string][]string) error
}

func ValidateRequestArgs(values map[string][]string, allowRawQueries bool) error {
	validators := []Validator{
		NewDateTimeValidator("start_at"),
		NewDateTimeValidator("end_at"),
		NewFilterByValidator(),
		NewSortByValidator(),
	}

	for _, v := range validators {
		if err := v.Validate(values); err != nil {
			return err
		}
	}

	return nil
}
