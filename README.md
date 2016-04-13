go-proto-collections
===========

```
protoc -I/usr/local/include -I. -I$GOPATH/src -I$GOPATH/src/github.com/willtrking/go-proto-collections --collections-go_out=logtostderr=true:./gopb/ *.proto
```