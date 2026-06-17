BINARY   := bin/kafkascope
PKG      := ./cmd/kafkascope
BROKERS  ?= localhost:9092
REFRESH  ?= 3s

.PHONY: all build run test vet lint clean deps \
        demo-up demo-seed demo-down

all: build

## Dependency management -------------------------------------------------------

deps:
	go mod tidy
	go mod verify

## Build & run -----------------------------------------------------------------

build: deps
	@mkdir -p bin
	go build -ldflags "-s -w" -o $(BINARY) $(PKG)

run: build
	$(BINARY) --brokers $(BROKERS) --refresh $(REFRESH)

## Quality checks --------------------------------------------------------------

test:
	go test -v -race ./...

vet:
	go vet ./...

## Demo environment ------------------------------------------------------------

demo-up:
	docker compose up -d
	@echo "Kafka starting — wait ~15 s for health check to pass."

demo-seed:
	bash scripts/seed.sh

demo-down:
	docker compose down -v

## Housekeeping ----------------------------------------------------------------

clean:
	rm -rf $(BINARY) bin/
