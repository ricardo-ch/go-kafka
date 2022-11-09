.PHONY: build
build:
	CGO_ENABLED=0 go build -a -ldflags '-s' -installsuffix cgo

.PHONY: test
test:
	go test -race -v ./...
	

.MOCKERY_PATH :=  $(shell  [ -z "$${GOBIN}" ] && echo $${GOPATH}/bin/mockery ||  echo $${GOBIN}/mockery; )

.PHONY: mocks
mocks:
ifeq (,$(shell which mockery))
	$(error "No mockery in PATH, consider doing brew install mockery")
else
	go mod vendor
	mockery --case "underscore" --dir vendor/github.com/Shopify/sarama --output ./mocks --case "underscore" --name="(ConsumerGroupHandler)|(SyncProducer)|(ConsumerGroup)|(ConsumerGroupClaim)|(ConsumerGroupSession)"
	mockery --case "underscore" --dir ./ --output ./mocks --name=StdLogger
endif

.PHONY: install
install:
	go get ./...