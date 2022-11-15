GO ?= go
GOTESTSUM ?= gotestsum

.PHONY: help
## This make command
help:
	@printf "command options\n\n"
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "make %-30s ## %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: build
## run the example
build:
	@$(GO) mod tidy
	@enable_chaos_test=true $(GO) test -run TestLocalDevOpsSimpleRestart
