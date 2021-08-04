### GOADAPTERS
#### Production ready adapters for services written in golang.

#### `goadapter` currently supports
- Kafka for pubsub

### Installation
Start your go module if not already done.
```
go mod init github.com/me/repo
```
Get `goadatper` as dependency.
```
go get github.com/workindia/goadapter
```
Finally, run 
```
go mod tidy
```
You can import `goadapters` now in your go module.

### Usage and Examples
This repository holds the examples in respective package as a quickstart guide. These examples are linked below -
- [Kafka Producer](/pubsub/examples/producer.go) 

### Testing
We maintain tests in golang standard `_test.go` files in respective package.
To test your package, run `go test ./package_name/test`
For example, to test pubsub package, run
```
go test ./pubsub/test
```

### Contribute
Fork this repository. Commit your changes and create pull request. Our team will check it for sure.
