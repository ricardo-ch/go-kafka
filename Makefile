.PHONY: build
build:
	CGO_ENABLED=0 go build -a -ldflags '-s' -installsuffix cgo

.PHONY: test
test:
	go test -v `go list ./...`

.MOCKERY_PATH :=  $(shell  [ -z "$${GOBIN}" ] && echo $${GOPATH}/bin/mockery ||  echo $${GOBIN}/mockery; )

.PHONY: mocks
mocks:
	dep ensure
	go get github.com/vektra/mockery/.../
	${.MOCKERY_PATH} -case "underscore" -dir vendor/github.com/Shopify/sarama -output ./mocks -case "underscore" -name="(ConsumerGroupHandler)|(SyncProducer)|(ConsumerGroup)|(ConsumerGroupClaim)|(ConsumerGroupSession)"
	${.MOCKERY_PATH} -case "underscore" -dir ./ -output ./mocks -name=StdLogger
