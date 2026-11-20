# Every directory containing a go.mod is a module (core + adapter submodules).
MODULES := $(shell find . -name go.mod -not -path './.git/*' -exec dirname {} \; | sort)

.PHONY: tidy test lint vet modules

modules:
	@echo $(MODULES)

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...) || exit 1; done

lint:
	@for m in $(MODULES); do (cd $$m && golangci-lint run ./...) || exit 1; done
