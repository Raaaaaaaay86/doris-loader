# Doris Stream Loader
[![en](https://img.shields.io/badge/lang-en-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.md)
[![zh](https://img.shields.io/badge/lang-zh-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.zh.md)

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