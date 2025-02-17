# Doris Stream Loader

[![en](https://img.shields.io/badge/lang-en-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.md)
[![zh](https://img.shields.io/badge/lang-zh-blue)](https://github.com/raaaaaaaay86/doris-loader/blob/main/README.zh.md)

# 安裝
```shell
go get github.com/raaaaaaaay86/doris-loader
```

# 關於

`doris-loader` 是一個使用 Apache Doris StreamLoad HTTP API 載入資料的package。目的在提供一種簡化且易讀的方式將資料載入到 Doris，而不是直接編寫原始的 HTTP 請求。

# 使用方法
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
`doris-loader`使用`InlineJson`作為預設載入格式，你可以使用`WithLoadFormat`來更改載入格式。

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
如果你想要使用csv格式載入資料，你應該指定`WithLoadFormat(Csv)`和`WithColumnSeparator`來設定欄位分隔符號，如果欄位分隔符號不是`\t`。最後需要使用`WithColumns`來指定對應於csv欄位的欄位名稱。

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
如果你想要使用csv格式載入資料並且首行為欄位名稱，你只需要指定`WithLoadFormat(CsvWithNames)`和`WithColumnSeparator`來設定欄位分隔符號，如果欄位分隔符號不是`\t`。