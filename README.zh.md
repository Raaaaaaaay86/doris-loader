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