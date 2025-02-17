package loader

import (
	"strings"
	"time"

	"github.com/raaaaaaaay86/doris-loader/enum"
	"github.com/raaaaaaaay86/doris-loader/enum/loadformat"
	"github.com/raaaaaaaay86/doris-loader/enum/protocol"
)


type StreamLoaderOption func(*StreamLoader) error

// WithLoadFormat sets the data format of the loaded file. It'll return an error if there has any value set before or provided an unexpected loadformat.Enum.
func WithLoadFormat(format loadformat.Enum) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if !enum.IsZero(loader.LoadFormat) && loader.LoadFormat != format {
			return ErrAmbiguousOption("LoadFormat")
		}

		loader.LoadFormat = format

		switch loader.LoadFormat {
		case loadformat.InlineJson:
			loader.Header["format"] = "json"
			loader.Header["read_json_by_line"] = true
		case loadformat.Csv:
			loader.Header["format"] = "csv"
			loader.Header["column_separator"] = ","
		case loadformat.CsvWithNames:
			loader.Header["format"] = "csv_with_names"
			loader.Header["column_separator"] = ","
		default:
			if enum.IsZero(format) {
				return ErrZeroValueOption("LoadFormat")
			}

			return ErrUnsupportValue(format)
		}

		return nil
	}
}

// WithProtocol sets the stream load protocol to determince using HTTP or HTTPS. It'll return an error if there has any value set before or provided an unexpected protocol.Enum.
func WithProtocol(p protocol.Enum) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if !enum.IsZero(loader.Protocol) && loader.Protocol != p {
			return ErrAmbiguousOption("Protocol")
		}

		switch p {
		case protocol.Http, protocol.Https:
			loader.Protocol = p
		default:
			if enum.IsZero(p) {
				return ErrZeroValueOption("Protocol")
			}

			return ErrUnsupportValue(p)
		}

		return nil
	}
}

// WithHeader sets the stream load header. It'll set whole header if there has no header set before. Otherwise, it'll merge the provided header with the existing header.
func WithHeader(m map[string]any) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.Header == nil {
			loader.Header = m
			return nil
		}

		for k, v := range m {
			loader.Header[k] = v
		}

		return nil
	}
}

// WithUsername sets the username for authentication. It'll return an error if there has any username set before.
func WithUsername(username string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.Username != "" && loader.Username != username {
			return ErrAmbiguousOption("Username")
		}

		loader.Username = username

		return nil
	}
}

// WithPassword sets the password for authentication. It'll return an error if there has any password set before.
func WithPassword(password string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.Password != "" && loader.Password != password{
			return ErrAmbiguousOption("Password")
		}

		loader.Password = password

		return nil
	}
}

// WithBeNodes sets the backend nodes for stream load. It'll return an error if there has any backend nodes set before.
func WithBeNodes(beNodes []string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if len(loader.BeNodes) != 0 {
			return ErrAmbiguousOption("BeNodes")
		}

		loader.BeNodes = beNodes

		return nil
	}
}

// WithColumns sets the columns name of CSV file. It'll return an error if there has any columns set before.
func WithColumns(columns []string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if _, ok := loader.Header["columns"]; ok {
			return ErrAmbiguousOption("Columns")
		}

		loader.Header["columns"] = strings.Join(columns, ",")

		return nil
	}
}

// WithMaxRetry sets the maximum retry count for stream load. It'll return an error if there has any max retry set before.
func WithMaxRetry(retry int) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.MaxRetry != 3 && loader.MaxRetry != retry { // 3 is the default value
			return ErrAmbiguousOption("MaxRetry")
		}

		loader.MaxRetry = retry

		return nil
	}
}

// WithRetryInterval sets the retry interval for stream load. It'll return an error if there has any retry interval set before.
func WithRetryInterval(interval time.Duration) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.RetryInterval != 1*time.Second && loader.RetryInterval != interval { // 1 second is the default value
			return ErrAmbiguousOption("RetryInterval")
		}

		loader.RetryInterval = interval

		return nil
	}
}

func WithLabel(label string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if oldLabel, ok := loader.Header["label"]; ok && oldLabel != label {
			return ErrAmbiguousOption("Label")
		}

		loader.Header["label"] = label

		return nil
	}
}