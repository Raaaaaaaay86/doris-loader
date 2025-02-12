package loader_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/raaaaaaaay86/doris-loader/enum/loadformat"
	"github.com/raaaaaaaay86/doris-loader/enum/protocol"
	"github.com/raaaaaaaay86/doris-loader/loader"
	"github.com/stretchr/testify/assert"
)

func TestNewStreamLoader(t *testing.T) {
	type testcase struct {
		FeNodes         []string
		Database        string
		Table           string
		Options         []loader.StreamLoaderOption
		ExpectFunc      func(testcase, *loader.StreamLoader, error)
		TestDescription string
	}

	testcases := []testcase{
		{
			TestDescription: "only pass required fields and without any options. The constructed loader should have set default value on optional fields",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options:         []loader.StreamLoaderOption{},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, ld)

				assert.Equal(t, tc.FeNodes, ld.FeNodes)
				assert.Empty(t, ld.BeNodes)

				assert.Equal(t, tc.Database, ld.Database)
				assert.Equal(t, tc.Table, ld.Table)

				assert.Equal(t, loadformat.InlineJson, ld.LoadFormat)

				assert.Equal(t, protocol.Http, ld.Protocol)

				assert.Equal(t, "", ld.Username)
				assert.Equal(t, "", ld.Password)

				assert.NotNil(t, ld.Header)
				assert.Equal(t, "100-continue", ld.Header["expect"])
			},
		},
		{
			TestDescription: "pass all fields and options. The constructed loader should have set value on all fields",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithLoadFormat(loadformat.InlineJson),
				loader.WithProtocol(protocol.Https),
				loader.WithUsername("my_username"),
				loader.WithPassword("my_password"),
				loader.WithHeader(map[string]any{
					"key1": "value1",
				}),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, ld)

				assert.Equal(t, tc.FeNodes, ld.FeNodes)
				assert.Empty(t, ld.BeNodes)

				assert.Equal(t, tc.Database, ld.Database)
				assert.Equal(t, tc.Table, ld.Table)

				assert.Equal(t, loadformat.InlineJson, ld.LoadFormat)

				assert.Equal(t, protocol.Https, ld.Protocol)

				assert.Equal(t, "my_username", ld.Username)
				assert.Equal(t, "my_password", ld.Password)

				assert.NotNil(t, ld.Header)
				assert.Equal(t, "100-continue", ld.Header["expect"])
				assert.NotNil(t, ld.Header["key1"])
				assert.Equal(t, "value1", ld.Header["key1"])
			},
		},
		{
			TestDescription: "pass empty list of FeNodes. The constructor should return error",
			FeNodes:         []string{},
			Database:        "my_database",
			Table:           "my_table",
			Options:         []loader.StreamLoaderOption{},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "frontend nodes are required")
			},
		},
		{
			TestDescription: "pass empty database. The constructor should return error",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "",
			Table:           "my_table",
			Options:         []loader.StreamLoaderOption{},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "database is required")
			},
		},
		{
			TestDescription: "pass empty table. The constructor should return error",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "",
			Options:         []loader.StreamLoaderOption{},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "table is required")
			},
		},
		{
			TestDescription: "should prevent ambiguous username option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithUsername("my_username"),
				loader.WithUsername("my_another_username"),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous username. are you going to use my_username or my_another_username?")
			},
		},
		{
			TestDescription: "should prevent ambiguous password option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithPassword("my_password"),
				loader.WithPassword("my_another_password"),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous password. there is already a password set")
			},
		},
		{
			TestDescription: "should prevent ambiguous protocol option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithProtocol(protocol.Http),
				loader.WithProtocol(protocol.Https),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous protocol. are you going to use http or https?")
			},
		},
		{
			TestDescription: "should prevent ambiguous load format option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithLoadFormat(loadformat.InlineJson),
				loader.WithLoadFormat(loadformat.Csv),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous load format. are you going to use inline_json or csv?")
			},
		},
		{
			TestDescription: "should prevent ambiguous load format option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithColumns([]string{"column_a", "column_b"}),
				loader.WithColumns([]string{"column_a", "column_b"}),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous columns. There has columns already set")
			},
		},
		{
			TestDescription: "should prevent ambiguous backend nodes option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithBeNodes([]string{"127.0.0.1:8040"}),
				loader.WithBeNodes([]string{"127.0.0.1:8041"}),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous backend nodes. there has already backend nodes set")
			},
		},
		{
			TestDescription: "should prevent ambiguous max retry option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithMaxRetry(4),
				loader.WithMaxRetry(5),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous max retry. there is already a max retry set")
			},
		},
		{
			TestDescription: "let user set same max try option twice, if the value is the same",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithMaxRetry(4),
				loader.WithMaxRetry(4),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, ld)

				assert.Equal(t, 4, ld.MaxRetry)
			},
		},
		{
			TestDescription: "should prevent ambiguous retry interval option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithRetryInterval(4 * time.Second),
				loader.WithRetryInterval(5 * time.Second),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "ambiguous retry interval. there is already a retry interval set")
			},
		},
		{
			TestDescription: "let user set same max try option twice, if the value is the same",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithRetryInterval(4 * time.Second),
				loader.WithRetryInterval(4 * time.Second),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, ld)
				assert.Equal(t, 4*time.Second, ld.RetryInterval)
			},
		},

		{
			TestDescription: "should prevent empty protocol option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithProtocol(protocol.Enum("")),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "provided protocol is zero value")
			},
		},
		{
			TestDescription: "should prevent empty load format option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithLoadFormat(loadformat.Enum("")),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "provided load format is zero value")
			},
		},
		{
			TestDescription: "should indicate unsupported protocol option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithProtocol(protocol.Enum("invalid_protocol")),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "unsupported protocol: invalid_protocol")
			},
		},
		{
			TestDescription: "should indicate unsupported load format option",
			FeNodes:         []string{"127.0.0.1:8030"},
			Database:        "my_database",
			Table:           "my_table",
			Options: []loader.StreamLoaderOption{
				loader.WithLoadFormat(loadformat.Enum("invalid_load_format")),
			},
			ExpectFunc: func(tc testcase, ld *loader.StreamLoader, err error) {
				assert.Error(t, err)
				assert.NotNil(t, ld)
				assert.EqualError(t, err, "unsupported load format: invalid_load_format")
			},
		},
	}

	for _, tc := range testcases {
		t.Log(tc.TestDescription)

		ld, err := loader.NewStreamLoader(tc.FeNodes, tc.Database, tc.Table, tc.Options...)
		tc.ExpectFunc(tc, ld, err)
	}
}

func TestStreamLoad(t *testing.T) {
	t.Log("stream load a file to Doris")

	feNodes := "127.0.0.1:8030"
	beNodes := "127.0.0.1:8040"
	username := "root"
	password := ""

	ld, err := loader.NewStreamLoader(
		strings.Split(feNodes, ","),
		"my_db",
		"users",
		loader.WithBeNodes(strings.Split(beNodes, ",")),
		loader.WithUsername(username),
		loader.WithPassword(password),
	)
	if err != nil {
		t.FailNow()
		return
	}

	temp, err := os.CreateTemp("", "test_stream_load_*")
	if err != nil {
		t.FailNow()
		return
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	lines := []string{
		`{"name": "John Doe", "age": 30}`,
	}
	for _, line := range lines {
		_, err = temp.WriteString(line + "\n")
		if err != nil {
			t.FailNow()
			return
		}
	}

	result, err := ld.LoadFile(context.Background(), temp.Name())
	if err != nil {
		t.Logf("stream load error: %s", err.Error())
		t.FailNow()
		return
	}

	if !result.IsSuccess() {
		t.Logf("error_url=%s message=%s", result.ErrorURL, result.Message)
		assert.True(t, result.IsSuccess())
	}
}

func TestStreamLoadWithCsvLoadFormat(t *testing.T) {
	t.Log("stream load a csv file to Doris")

	feNodes := "127.0.0.1:8030"
	beNodes := "127.0.0.1:8040"
	username := "root"
	password := ""

	ld, err := loader.NewStreamLoader(
		strings.Split(feNodes, ","),
		"my_db",
		"users",
		loader.WithBeNodes(strings.Split(beNodes, ",")),
		loader.WithUsername(username),
		loader.WithPassword(password),
		loader.WithLoadFormat(loadformat.Csv),
		loader.WithColumns([]string{"name", "age"}),
	)
	if err != nil {
		t.FailNow()
		return
	}

	temp, err := os.CreateTemp("", "test_stream_load_csv_*")
	if err != nil {
		t.FailNow()
		return
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	lines := []string{
		`Jenny Chang,50`,
	}
	for _, line := range lines {
		_, err = temp.WriteString(line + "\n")
		if err != nil {
			t.FailNow()
			return
		}
	}

	result, err := ld.LoadFile(context.Background(), temp.Name())
	if err != nil {
		t.Logf("stream load error: %s", err.Error())
		t.FailNow()
		return
	}

	if !result.IsSuccess() {
		t.Logf("error_url=%s message=%s", result.ErrorURL, result.Message)
		assert.True(t, result.IsSuccess())
	}

	resultStr, _ := json.MarshalIndent(result, "", "  ")
	t.Log(string(resultStr))
}
func TestStreamLoadWithCsvWithNamesLoadFormat(t *testing.T) {
	t.Log("stream load a csv file to Doris")

	feNodes := "127.0.0.1:8030"
	beNodes := "127.0.0.1:8040"
	username := "root"
	password := ""

	ld, err := loader.NewStreamLoader(
		strings.Split(feNodes, ","),
		"my_db",
		"users",
		loader.WithBeNodes(strings.Split(beNodes, ",")),
		loader.WithUsername(username),
		loader.WithPassword(password),
		loader.WithLoadFormat(loadformat.CsvWithNames),
	)
	if err != nil {
		t.FailNow()
		return
	}

	temp, err := os.CreateTemp("", "test_stream_load_csv_with_names_*")
	if err != nil {
		t.FailNow()
		return
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	lines := []string{
		`name, age`,
		`Jenny Wong,50`,
	}
	for _, line := range lines {
		_, err = temp.WriteString(line + "\n")
		if err != nil {
			t.FailNow()
			return
		}
	}

	result, err := ld.LoadFile(context.Background(), temp.Name())
	if err != nil {
		t.Logf("stream load error: %s", err.Error())
		t.FailNow()
		return
	}

	if !result.IsSuccess() {
		t.Logf("error_url=%s message=%s", result.ErrorURL, result.Message)
		assert.True(t, result.IsSuccess())
	}

	resultStr, _ := json.MarshalIndent(result, "", "  ")
	t.Log(string(resultStr))
}
