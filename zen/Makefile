BIN := bin/zen

.PHONY: build install test clean

# Build bin/zen for this machine. GOWORK=off builds the module the way
# "go install github.com/dungnh3/kitchen/zen@latest" does, whatever go.work
# says.
build:
	GOWORK=off go build -trimpath -o $(BIN) .

# Install zen in $(go env GOBIN), or in $(go env GOPATH)/bin when GOBIN is
# not set. That directory must be in PATH.
install:
	GOWORK=off go install -trimpath .

test:
	go test -race -count=1 ./...

clean:
	rm -f $(BIN)
