# source-mysql

MySQL source on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk): full refresh, incremental by cursor and CDC from the binlog. The integration tests start a MySQL 8.0 testcontainer, so `go test ./...` needs Docker; `go test -short ./...` skips them.

```bash
task test CONNECTOR=source-mysql
task build CONNECTOR=source-mysql
task spec CONNECTOR=source-mysql
```
