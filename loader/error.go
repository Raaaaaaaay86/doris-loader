package loader

import (
	"fmt"
)

var (
	ErrAmbiguousOption = func(field string) error {
		return fmt.Errorf("ambiguous option: %s", field)
	}
	ErrZeroValueOption = func(field string) error {
		return fmt.Errorf("option is zero value: %s", field)
	}
	ErrUnsupportValue = func(value any) error {
		return fmt.Errorf("unsupported value: %v", value)
	}
	ErrMissingRequiredValue = func(value any) error {
		return fmt.Errorf("missing required value: %v", value)
	}
)
