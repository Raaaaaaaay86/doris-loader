package loader

import (
	"fmt"

	"github.com/raaaaaaaay86/doris-loader/enum"
	"github.com/raaaaaaaay86/doris-loader/enum/loadformat"
	"github.com/raaaaaaaay86/doris-loader/enum/protocol"
)


type StreamLoaderOption func(*StreamLoader) error

func WithLoadFormat(format loadformat.Enum) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if !enum.IsZero(loader.LoadFormat) {
			return fmt.Errorf("ambiguous load format. are you going to use %s or %s?", loader.LoadFormat, format)
		}

		loader.LoadFormat = format

		switch loader.LoadFormat {
		case loadformat.InlineJson:
			loader.Header["format"] = "json"
			loader.Header["read_json_by_line"] = true
		default:
			if enum.IsZero(format) {
				return fmt.Errorf("provided load format is zero value")
			}

			return fmt.Errorf("unsupported load format: %s", format)
		}

		return nil
	}
}

func WithProtocol(p protocol.Enum) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if !enum.IsZero(loader.Protocol) {
			return fmt.Errorf("ambiguous protocol. are you going to use %s or %s?", loader.Protocol, p)
		}

		switch loader.Protocol {
		case protocol.Http, protocol.Https:
			loader.Protocol = p
		default:
			if enum.IsZero(p) {
				return fmt.Errorf("provided protocol is zero value")
			}

			return fmt.Errorf("unsupported protocol: %s", p)
		}

		return nil
	}
}

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

func WithUsername(username string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.Username != "" {
			return fmt.Errorf("ambiguous username. are you going to use %s or %s?", loader.Username, username)
		}

		loader.Username = username

		return nil
	}
}

func WithPassword(password string) StreamLoaderOption {
	return func(loader *StreamLoader) error {
		if loader.Password != "" {
			return fmt.Errorf("ambiguous password. there is already a password set")
		}

		loader.Password = password

		return nil
	}
}
