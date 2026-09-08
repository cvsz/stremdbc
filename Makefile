.PHONY: all build run test test-race test-coverage tidy deps clean docker-build docker-run docker-stop docker-compose-up docker-compose-down docker-compose-monitoring fmt fmt-check lint vet verify help

GOCMD ?= go
GOBUILD := $(GOCMD) build
GORUN := $(GOCMD) run
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod
STATICCHECK_VERSION ?= v0.8.1
STATICCHECK := $(GORUN) honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
GOSEC_VERSION ?= v2.29.0
GOSEC := $(GORUN) github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)
BINARY_NAME ?= stremdbc
MAIN_PATH := ./cmd/stremdbc
VERSION ?= 0.6.0
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

all: verify

build:
	$(GOBUILD) $(LDFLAGS) -trimpath -o $(BINARY_NAME) $(MAIN_PATH)

run:
	$(GORUN) $(LDFLAGS) $(MAIN_PATH)

test:
	$(GOTEST) -count=1 ./...

test-race:
	$(GOTEST) -race -count=1 ./...

test-coverage:
	$(GOTEST) -count=1 -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

tidy:
	$(GOMOD) tidy

deps:
	$(GOMOD) download
	$(GOMOD) verify

clean:
	rm -f $(BINARY_NAME) coverage.out coverage.html

docker-build:
	docker build --pull -t stremdbc:local .

docker-run:
	docker run --rm -d -p 127.0.0.1:8080:8080 --env-file .env --name stremdbc stremdbc:local

docker-stop:
	-docker rm -f stremdbc

docker-compose-up:
	docker compose up -d --build

docker-compose-down:
	docker compose down

docker-compose-monitoring:
	docker compose --profile monitoring up -d

fmt:
	$(GOCMD) fmt ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt required:"; gofmt -l .; exit 1; }

lint: fmt-check
	$(STATICCHECK) ./...
	$(GOSEC) ./...
	$(GOCMD) vet ./...

vet:
	$(GOCMD) vet ./...

verify: fmt-check
	$(GOMOD) verify
	$(GOMOD) tidy -diff
	$(GOTEST) -race -count=1 ./...
	$(STATICCHECK) ./...
	$(GOSEC) ./...
	$(GOCMD) vet ./...
	$(GOBUILD) -trimpath -o /tmp/stremdbc-verify $(MAIN_PATH)

help:
	@echo "STREMDBC Makefile commands:"
	@echo "  build                   Build the binary"
	@echo "  run                     Run the service"
	@echo "  test                    Run unit tests"
	@echo "  test-race               Run race-enabled tests"
	@echo "  test-coverage           Generate an HTML coverage report"
	@echo "  tidy / deps             Maintain or download Go modules"
	@echo "  fmt / fmt-check / lint  Format and run standard Go checks"
	@echo "  vet                     Run go vet"
	@echo "  verify                  Run the complete local verification gate"
	@echo "  docker-build / run      Build or run the container"
	@echo "  docker-compose-up/down  Manage the default Compose stack"
