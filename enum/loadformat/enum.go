package loadformat

type Enum string

const (
	InlineJson   Enum = "inline_json"
	Csv          Enum = "csv"
	CsvWithNames Enum = "csv_with_names"
)
