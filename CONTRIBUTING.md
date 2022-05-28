# Contributing to Barco Streams

Glad to know that you're interested in contributing to Barco.

## Questions, bugs and features

Whether it's a bug report, feature request or a question, you're welcome to [raise an
issue](https://github.com/barcostreams/barco/issues).

We aim to respond to your issues soonest. If you wish to receive a faster response, we recommend you always describe
your steps and provide information about your environment in the bug reports. And if you're proposing a new feature,
it'll help us to evaluate the priority if you explain why you need it.

## Environment setup

You need go 1.17+ to build Barco from source.

### Run unit tests

Executing `go test` command with no additional flags will run the unit tests. Unit tests should complete in few seconds.

```shell
go test -v ./...
```

### Run integration tests

Set the build tag `integration` to run the integration tests. You can expect integration tests to take less than 5
minutes to complete.

```shell
go test -v -tags=integration -p=1 ./internal/test/integration
```

Note that in macOS, you need to manually create the alias for the loopback addresses `127.0.0.2` to `127.0.0.6` in order
to run the integration tests. For example:

```shell
for i in {2..6}; do sudo ifconfig lo0 alias 127.0.0.$i up; done
```

Development in Windows has not been tested yet. We recommend using the Windows Subsystem for Linux (WSL) to build and
run Barco in your Windows development environment.
