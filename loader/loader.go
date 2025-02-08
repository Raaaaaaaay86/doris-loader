package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/raaaaaaaay86/doris-loader/enum"
	"github.com/raaaaaaaay86/doris-loader/enum/loadformat"
	protocol "github.com/raaaaaaaay86/doris-loader/enum/protocol"
)

type StreamLoader struct {
	Protocol   protocol.Enum   // stream protocol. (default: Http)
	FeNodes    []string        // Frontend endpoints (e.g 127.0.0.1:8030)
	BeNodes    []string        // Backend endpoints (e.g 127.0.0.1:8040)
	Username   string          // Username
	Password   string          // Password
	Database   string          // Database name
	Table      string          // Table name
	Header     map[string]any  // Stream load header
	LoadFormat loadformat.Enum // Data format of loaded file (default: InlineJson)
}

// NewStreamLoader creates a new stream loader.
func NewStreamLoader(
	feNodes []string,
	database string,
	table string,
	options ...StreamLoaderOption,
) (*StreamLoader, error) {
	loader := StreamLoader{
		FeNodes:  feNodes,
		Database: database,
		Table:    table,
		Header: map[string]any{
			"expect": "100-continue",
		},
	}

	if err := loader.checkRequiredFields(); err != nil {
		return &loader, err
	}

	for _, option := range options {
		if err := option(&loader); err != nil {
			return &loader, err
		}
	}

	if enum.IsZero(loader.LoadFormat) {
		if err := WithLoadFormat(loadformat.InlineJson)(&loader); err != nil {
			return &loader, err
		}
	}

	if enum.IsZero(loader.Protocol) {
		if err := WithProtocol(protocol.Http)(&loader); err != nil {
			return &loader, err
		}
	}

	return &loader, nil
}

// LoadFile stream loads a file to Doris.
//	
//	loader, err := loader.NewStreamLoader(
//		[]string{"127.0.0.1:8030"},
//		"db_name",
//		"table_name",
//		WithUsername("root"),
//		WithPassword("changeme"),
//	)
//	if err != nil {
//		return err
//	}
//	
//	// Return stream load result
//	result, err := loader.LoadFile(context.TODO(), "path/to/file")
//	if err != nil {
//		return err
//	}
//	
//	if result.IsSuccess() {
//		// Do something for fail result...
//	}
func (s StreamLoader) LoadFile(
	ctx context.Context,
	filename string,
) (*StreamLoadResult, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	req, err := s.buildRequest(file)
	if err != nil {
		return nil, err
	}

	result, err := s.doRequest(req)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// checkRequiredFields checks if required fields are set.
func (s StreamLoader) checkRequiredFields() error {
	if len(s.FeNodes) == 0 {
		return fmt.Errorf("frontend nodes are required")
	}

	if s.Database == "" {
		return fmt.Errorf("database is required")
	}

	if s.Table == "" {
		return fmt.Errorf("table is required")
	}

	return nil
}

// buildRequest builds a http request for stream load.
func (s StreamLoader) buildRequest(payload io.Reader) (*http.Request, error) {
	url := fmt.Sprintf(
		"%s://%s/api/%s/%s/_stream_load",
		s.Protocol,
		s.FeNodes[0],
		s.Database,
		s.Table,
	)

	req, err := http.NewRequest(http.MethodPut, url, io.NopCloser(payload))
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(s.Username, s.Password)
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(payload), nil
	}

	if s.Header != nil {
		for k, v := range s.Header {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	return req, nil
}

// doRequest sends a stream load http request.
func (s StreamLoader) doRequest(req *http.Request) (*StreamLoadResult, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			redirectTo, _ := url.Parse(
				fmt.Sprintf(
					"%s://%s:%s@%s/api/%s/%s/_stream_load",
					s.Protocol,
					s.Username,
					s.Password,
					s.FeNodes[0],
					s.Database,
					s.Table,
				),
			)

			req.URL = redirectTo

			return nil
		},
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()


	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var result StreamLoadResult
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
