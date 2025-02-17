# Doris Stream Loader
[![en](https://img.shields.io/badge/lang-en-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.md)
[![zh](https://img.shields.io/badge/lang-zh-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.zh.md)

# Installation
```shell
go get github.com/raaaaaaaay86/doris-loader
```

# About

`doris-loader` is a tool for using Apache Doris StreamLoad HTTP API to load data. This package aims to provide a simplified and expressive way to load data into Doris instead of writing raw HTTP request manually.

# Usage
```go
ld, err := loader.NewStreamLoader(
  []string{"127.0.0.1:8030"},
  "database_name",
  "table_name",
  loader.WithBeNodes([]string{"127.0.0.1:8040"}), // force redirect stream load reqeust to designated BE nodes
  loader.WithUsername(username),
  loader.WithPassword(password),
)
if err != nil {
  return err
}

result, err := ld.LoadFile(context.Background(), "path/to/file")
// skip...
```
`doris-loader` use `InlineJson` as default load format, you can change it by using `WithLoadFormat`.

```go
ld, err := loader.NewStreamLoader(
  []string{"127.0.0.1:8030"},
  "database_name",
  "table_name",
  loader.WithUsername(username),
  loader.WithPassword(password),
  loader.WithLoadFormat(loader.Csv),
  loader.WithColumnSeparator(","),
  loader.WithColumns([]string{"col1", "col2", "col3"}),
)
if err != nil {
  return err
}
```
If you want to load data by csv format, you should specify `WithLoadFormat(Csv)` and `WithColumnSeparator` to set the column separator if the column separator is not `\t`. Lastly, you should use `WithColumns` to specify the column names which correspond to the csv columns.

```go
ld, err := loader.NewStreamLoader(
  []string{"127.0.0.1:8030"},
  "database_name",
  "table_name",
  loader.WithUsername(username),
  loader.WithPassword(password),
  loader.WithLoadFormat(loader.CsvWithNames),
  loader.WithColumnSeparator(","),
)
if err != nil {
  return err
}
```
If you want to load data by csv format with column names at first line, you should only specify `WithLoadFormat(CsvWithNames)` and `WithColumnSeparator` to set the column separator if the column separator is not `\t`.
