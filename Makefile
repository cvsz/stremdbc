.PHONY: build run test clean docker docker-up docker-down help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=stremdbc
MAIN_PATH=./cmd/stremdbc

# Build flags
LDFLAGS=-ldflags "-X main.version=0.1.0"

all: build

## build: Build the binary
build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)

## run: Run the server
run:
	$(GORUN) $(LDFLAGS) $(MAIN_PATH)

## test: Run tests
test:
	$(GOTEST) -v ./...

## test-coverage: Run tests with coverage
test-coverage:
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

## tidy: Tidy go modules
tidy:
	$(GOMOD) tidy

## deps: Download dependencies
deps:
	$(GOMOD) download

## clean: Clean build artifacts
clean:
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

## docker-build: Build Docker image
docker-build:
	docker build -t stremdbc:latest .

## docker-run: Run Docker container
docker-run:
	docker run -d -p 8080:8080 -p 1935:1935 --name stremdbc stremdbc:latest

## docker-stop: Stop Docker container
docker-stop:
	docker stop stremdbc
	docker rm stremdbc

## docker-compose-up: Start with docker-compose
docker-compose-up:
	docker-compose up -d

## docker-compose-down: Stop with docker-compose
docker-compose-down:
	docker-compose down

## docker-compose-monitoring: Start with monitoring profile
docker-compose-monitoring:
	docker-compose --profile monitoring up -d

## fmt: Format code
fmt:
	$(GOCMD) fmt ./...

## lint: Run linter
lint:
	golangci-lint run

## vet: Run go vet
vet:
	$(GOCMD) vet ./...

## help: Show this help message
help:
	@echo "STREMDBC Makefile Commands:"
	@echo ""
	@echo "  build              - Build the binary"
	@echo "  run                - Run the server"
	@echo "  test               - Run tests"
	@echo "  test-coverage      - Run tests with coverage report"
	@echo "  tidy               - Tidy go modules"
	@echo "  deps               - Download dependencies"
	@echo "  clean              - Clean build artifacts"
	@echo "  docker-build       - Build Docker image"
	@echo "  docker-run         - Run Docker container"
	@echo "  docker-stop        - Stop Docker container"
	@echo "  docker-compose-up  - Start with docker-compose"
	@echo "  docker-compose-down- Stop with docker-compose"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  vet                - Run go vet"
	@echo ""
