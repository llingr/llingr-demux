UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Race detector works on macOS (any arch) and Linux x86_64
# Disabled on Linux ARM64 due to ThreadSanitizer VMA limitation
# Short mode reduces iterations for memory-constrained systems
ifeq ($(UNAME_S)-$(filter aarch64 arm%,$(UNAME_M)),Linux-$(UNAME_M))
  RACE :=
  SHORT := -short
else
  RACE := -race
  SHORT :=
endif

.PHONY: default build test integration cover bench fuzz

default: build test integration

build:
	go build ./...
	go vet ./...

test:
	go clean -testcache
	go test $(RACE) $(SHORT) -coverprofile=coverage.out ./demux/...
	go tool cover -func=coverage.out
	# integration tests and broker-specific benchmarks in repo: llingr-demux-tests"

integration:
	go test $(RACE) $(SHORT) ./tests/...

cover: test
	go tool cover -html=coverage.out

bench:
	@echo "==> Running benchmarks..."
	go test -run=^$$ -bench=. -benchmem ./demux/alloc
	go test -run=^$$ -bench=. -benchmem ./demux/pipeline
	go test -run=^$$ -bench=. -benchmem ./demux/pipeline/prev
	go test -run=^$$ -bench=. -benchmem ./demux/metrics
	go test -run=^$$ -bench=. -benchmem ./demux/offset

fuzz:
	@echo "==> Running fuzz tests..."
	go test -fuzz=Fuzz_CalculateCollectBufferSize -fuzztime=8s ./demux/metrics && \
	go test -fuzz=Fuzz_OffsetsTracker_HasPendingCommits -fuzztime=8s ./demux/offset && \
	go test -fuzz=Fuzz_clampPartitionsCount -fuzztime=8s ./demux/offset && \
	go test -fuzz=Fuzz_CalcWorkItemPoolSize -fuzztime=8s ./demux/alloc

verify: build fuzz bench test
